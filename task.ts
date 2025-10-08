
import { Static, Type, TSchema } from '@sinclair/typebox';
import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType, InputFeatureCollection } from '@tak-ps/etl';
import { XMLParser } from 'fast-xml-parser';
import { createHash } from 'crypto';

const Environment = Type.Object({
    RSS_URL: Type.String({
        description: 'CAP-NZ RSS or Atom feed URL'
    }),
    Headers: Type.Optional(Type.Array(Type.Object({
        key: Type.String(),
        value: Type.String()
    }))),
    Timeout: Type.Number({
        default: 30000,
        description: 'Request timeout in milliseconds'
    }),
    Retries: Type.Number({
        default: 2,
        description: 'Number of retry attempts on failure'
    })
});

type SupportedGeometry = { type: 'Point'; coordinates: number[] } | { type: 'LineString'; coordinates: number[][] } | { type: 'Polygon'; coordinates: number[][][] };

interface CAPAlert {
    identifier: string;
    sender: string;
    sent: string;
    status: string;
    msgType: string;
    scope: string;
    info: {
        category: string;
        event: string;
        urgency: string;
        severity: string;
        certainty: string;
        senderName: string;
        headline: string;
        description: string;
        instruction?: string;
        responseType?: string;
        onset?: string;
        expires?: string;
        web?: string;
        area: {
            areaDesc: string;
            polygon?: string | string[];
            circle?: string;
        };
        colorCode?: string;
    };
    signature?: {
        issuer?: string;
        subject?: string;
        validUntil?: string;
        fingerprint?: string;
    };
}

export default class Task extends ETL {
    static name = 'etl-capnz';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    private static readonly CATEGORY_MAP: Record<string, string> = {
        'Geo': 'Geophysical (including landslide)',
        'Met': 'Meteorological (including flood)',
        'Safety': 'General emergency and public safety',
        'Security': 'Law enforcement, military, homeland and local/private security',
        'Rescue': 'Rescue and recovery',
        'Fire': 'Fire suppression and rescue',
        'Health': 'Medical and public health',
        'Env': 'Pollution and other environmental hazards',
        'Transport': 'Public and private transportation',
        'Infra': 'Utility, telecommunication, other non-transport infrastructure',
        'CBRNE': 'Chemical, Biological, Radiological, Nuclear or High-Yield Explosive threat or attack',
        'Other': 'Other events'
    };

    private static readonly EVENT_MAP: Record<string, string> = {
        'storm': 'Storm',
        'hail': 'Hail',
        'rainfall': 'Rainfall',
        'snowfall': 'Snowfall',
        'thunderstorm': 'Thunderstorm',
        'tornado': 'Tornado',
        'tropCyclone': 'Tropical Cyclone',
        'tropStorm': 'Tropical Storm',
        'winterStorm': 'Winter Storm',
        'weather': 'Weather',
        'temperature': 'Temperature',
        'coldOutbreak': 'Cold Outbreak',
        'heatWave': 'Heat Wave',
        'frost': 'Frost',
        'windChill': 'Wind Chill',
        'wind': 'Wind',
        'avLightning': 'Airport Lightning Threat',
        'avThunder': 'Airport Thunder Threat',
        'fireWeather': 'Fire Weather',
        'flood': 'Flood',
        'flashFlood': 'Flash Flood',
        'highWater': 'High Water Level',
        'stormSurge': 'Storm Surge',
        'riverFlood': 'River Flood',
        'earthquake': 'Earthquake',
        'tsunami': 'Tsunami',
        'landTsunami': 'Land Threat Tsunami',
        'beachTsunami': 'Beach Threat Tsunami',
        'marine': 'Marine',
        'galeWind': 'Gale Wind',
        'hurricFrcWnd': 'Hurricane Force Wind',
        'iceberg': 'Iceberg',
        'largeSurf': 'Large Coastal Surf',
        'largeSwell': 'Large Swell Waves',
        'squall': 'Squall',
        'stormFrcWind': 'Storm Force Wind',
        'strongWind': 'Strong Wind',
        'waterspout': 'Waterspout',
        'snow': 'Snow'
    };

    private static readonly ICON_PREFIX = 'bb4df0a6-ca8d-4ba8-bb9e-3deb97ff015e:';
    private static readonly DEFAULT_ICON = 'Incidents/INC.38.NaturalDisaster3.InformationOnly.png';
    private static readonly ICON_MAP: Record<string, string> = {
        'snow': 'NaturalHazards/NH.07.Snow.png',
        'snowfall': 'NaturalHazards/NH.07.Snow.png',
        'rain': 'NaturalHazards/NH.05.HeavyRain.png',
        'rainfall': 'NaturalHazards/NH.05.HeavyRain.png',
        'wind': 'NaturalHazards/NH.04.StrongWind.png',
        'storm': 'NaturalHazards/NH.06.ElectricalStorm.png',
        'thunderstorm': 'NaturalHazards/NH.06.ElectricalStorm.png',
        'tornado': 'NaturalHazards/NH.16.Tornado.png',
        'tropCyclone': 'NaturalHazards/NH.09.TropicalCyclone.png',
        'tropStorm': 'NaturalHazards/NH.09.TropicalCyclone.png',
        'flood': 'NaturalHazards/NH.01.Flood.png',
        'flashFlood': 'NaturalHazards/NH.01.Flood.png',
        'earthquake': 'NaturalHazards/NH.24.Earthquake.png',
        'tsunami': 'NaturalHazards/NH.03.Tsunami.png',
        'marine': 'Incidents/INC.24.Marine.png',
        'fire': 'Incidents/INC.35.Fire.png',
        'fireWeather': 'Incidents/INC.37.Fire.Vegetation.png',
        'landslide': 'NaturalHazards/NH.18.Landslide.png',
        'stormSurge': 'NaturalHazards/NH.02.StormSurge.png',
        'ice': 'NaturalHazards/NH.08.Ice.png',
        'drought': 'NaturalHazards/NH.22.Drought.png',
        'biosecurity': 'NaturalHazards/NH.23.Biosecurity.png',
        'hail': 'NaturalHazards/NH.06.ElectricalStorm.png',
        'winterStorm': 'NaturalHazards/NH.07.Snow.png',
        'weather': 'Incidents/INC.38.NaturalDisaster1.Urgent.png',
        'temperature': 'Incidents/INC.38.NaturalDisaster1.Urgent.png',
        'coldOutbreak': 'NaturalHazards/NH.08.Ice.png',
        'heatWave': 'Incidents/INC.38.NaturalDisaster1.Urgent.png',
        'frost': 'NaturalHazards/NH.08.Ice.png',
        'windChill': 'NaturalHazards/NH.08.Ice.png',
        'avLightning': 'NaturalHazards/NH.06.ElectricalStorm.png',
        'avThunder': 'NaturalHazards/NH.06.ElectricalStorm.png',
        'highWater': 'NaturalHazards/NH.01.Flood.png',
        'riverFlood': 'NaturalHazards/NH.01.Flood.png',
        'landTsunami': 'NaturalHazards/NH.03.Tsunami.png',
        'beachTsunami': 'NaturalHazards/NH.03.Tsunami.png',
        'galeWind': 'NaturalHazards/NH.04.StrongWind.png',
        'hurricFrcWnd': 'NaturalHazards/NH.04.StrongWind.png',
        'iceberg': 'Incidents/INC.24.Marine.png',
        'largeSurf': 'Incidents/INC.24.Marine.png',
        'largeSwell': 'Incidents/INC.24.Marine.png',
        'squall': 'NaturalHazards/NH.04.StrongWind.png',
        'stormFrcWind': 'NaturalHazards/NH.04.StrongWind.png',
        'strongWind': 'NaturalHazards/NH.04.StrongWind.png',
        'waterspout': 'NaturalHazards/NH.16.Tornado.png'
    };

    private static readonly CERT_DEFAULTS = {
        ISSUER: 'cap.metservice.com',
        SUBJECT: 'METEOROLOGICAL SERVICE OF NEW ZEALAND LIMITED',
        VALID_UNTIL: '2025-10-23'
    };

    private getCategoryDescription(categoryCode: string): string {
        return Task.CATEGORY_MAP[categoryCode] || categoryCode || 'Unknown';
    }

    private getEventDescription(eventCode: string): string {
        return Task.EVENT_MAP[eventCode] || eventCode || 'Unknown';
    }

    private getEventIcon(eventType: string, category?: string): string {
        if (category === 'Health') {
            return `${Task.ICON_PREFIX}Incidents/INC.60.GHS08.HealthHazard.png`;
        }
        if (category === 'Fire') {
            return `${Task.ICON_PREFIX}Incidents/INC.35.Fire.png`;
        }
        const iconFile = Task.ICON_MAP[eventType] || Task.DEFAULT_ICON;
        return `${Task.ICON_PREFIX}${iconFile}`;
    }

    private parsePolygonString(polygonStr: string): number[][][] {
        if (!polygonStr || typeof polygonStr !== 'string') {
            throw new Error('Empty or invalid polygon string');
        }
        
        const trimmed = polygonStr.trim();
        if (!trimmed) {
            throw new Error('Empty polygon string after trimming');
        }
        
        const coordPairs = trimmed.split(/\s+/);
        const points: number[][] = [];
        const invalidPairs: string[] = [];
        
        for (const pair of coordPairs) {
            if (!pair || !pair.includes(',')) {
                if (pair) invalidPairs.push(pair);
                continue;
            }
            
            const parts = pair.split(',');
            if (parts.length !== 2) {
                invalidPairs.push(pair);
                continue;
            }
            
            const [latStr, lonStr] = parts;
            if (!latStr?.trim() || !lonStr?.trim()) {
                invalidPairs.push(pair);
                continue;
            }
            
            const lat = parseFloat(latStr.trim());
            const lon = parseFloat(lonStr.trim());
            
            if (isNaN(lat) || isNaN(lon)) {
                invalidPairs.push(pair);
                continue;
            }
            
            if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
                invalidPairs.push(pair);
                continue;
            }
            
            points.push([lon, lat]); // GeoJSON uses [lon, lat]
        }
        
        if (invalidPairs.length > 0) {
            throw new Error(`Invalid coordinate pairs: ${invalidPairs.slice(0, 3).join(', ')}${invalidPairs.length > 3 ? '...' : ''}`);
        }
        
        if (points.length < 3) {
            throw new Error(`Insufficient valid points: ${points.length} (minimum 3 required)`);
        }
        
        // Ensure polygon is closed
        if (points[0][0] !== points[points.length - 1][0] || points[0][1] !== points[points.length - 1][1]) {
            points.push([...points[0]]);
        }
        
        return [points];
    }

    private parseCircleString(circleStr: string): { center: number[]; radius: number } | null {
        if (!circleStr || typeof circleStr !== 'string') {
            return null;
        }
        
        const parts = circleStr.trim().split(' ');
        if (parts.length >= 2) {
            const [latStr, lonStr] = parts[0].split(',');
            const radius = parseFloat(parts[1]);
            if (latStr && lonStr) {
                const lat = parseFloat(latStr);
                const lon = parseFloat(lonStr);
                if (!isNaN(lat) && !isNaN(lon) && !isNaN(radius) && 
                    lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && radius > 0) {
                    return { center: [lon, lat], radius };
                }
            }
        }
        return null;
    }

    private calculatePolygonCentroid(coordinates: number[][][]): number[] {
        const points = coordinates[0];
        if (points.length < 3) {
            return [0, 0]; // Fallback for invalid polygon
        }
        
        // Use proper geometric centroid calculation
        let area = 0;
        let cx = 0;
        let cy = 0;
        
        for (let i = 0; i < points.length - 1; i++) {
            const x0 = points[i][0];
            const y0 = points[i][1];
            const x1 = points[i + 1][0];
            const y1 = points[i + 1][1];
            
            const a = x0 * y1 - x1 * y0;
            area += a;
            cx += (x0 + x1) * a;
            cy += (y0 + y1) * a;
        }
        
        area *= 0.5;
        if (Math.abs(area) < 1e-10) {
            // Fallback to arithmetic mean for degenerate polygons
            let x = 0, y = 0;
            for (const point of points) {
                x += point[0];
                y += point[1];
            }
            return [x / points.length, y / points.length];
        }
        
        cx /= (6 * area);
        cy /= (6 * area);
        
        return [cx, cy];
    }



    private async fetchWithRetry(url: URL, headers: Record<string, string>, timeout: number, retries: number): Promise<Response> {
        for (let attempt = 0; attempt <= retries; attempt++) {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), timeout);
            
            try {
                const res = await fetch(url, {
                    method: 'GET',
                    headers,
                    signal: controller.signal
                });
                
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status}: ${res.statusText}`);
                }
                
                return res;
            } catch (error) {
                if (attempt === retries) throw error;
                await new Promise(resolve => setTimeout(resolve, 1000 * (attempt + 1)));
            } finally {
                clearTimeout(timeoutId);
            }
        }
    }

    private async parseXML(xmlText: string): Promise<CAPAlert | null> {
        try {
            const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@' });
            const parsed = parser.parse(xmlText);
            
            const alert = parsed.alert;
            if (!alert) {
                console.log('No alert element found in XML');
                return null;
            }
            
            const identifier = alert.identifier || '';
            const sender = alert.sender || '';
            const sent = alert.sent || '';
            const status = alert.status || '';
            const msgType = alert.msgType || '';
            const scope = alert.scope || '';
            
            const info = alert.info;
            if (!info) {
                return null;
            }
            
            if (!identifier || !sender || !sent) {
                return null;
            }
            
            const category = info.category || '';
            const event = info.event || '';
            const urgency = info.urgency || '';
            const severity = info.severity || '';
            const certainty = info.certainty || '';
            const senderName = info.senderName || '';
            const headline = info.headline || '';
            const description = info.description || '';
            const instruction = info.instruction || '';
            const responseType = info.responseType || '';
            const onset = info.onset || '';
            const expires = info.expires || '';
            const web = info.web || '';
            
            const area = info.area || {};
            const areaDesc = area.areaDesc || '';
            const polygon = area.polygon || '';
            const circle = area.circle || '';
            
            // Extract ColourCodeHex parameter
            let colorCode = null;
            if (info.parameter) {
                const params = Array.isArray(info.parameter) ? info.parameter : [info.parameter];
                // Prioritize ColourCodeHex over ColourCode
                const hexParam = params.find((p: { valueName: string; value: string }) => p.valueName === 'ColourCodeHex');
                if (hexParam) {
                    colorCode = hexParam.value;
                } else {
                    const colorParam = params.find((p: { valueName: string; value: string }) => p.valueName === 'ColourCode');
                    if (colorParam) {
                        const colorMap: Record<string, string> = {
                            'Red': '#FF0000',
                            'Orange': '#FF8918',
                            'Yellow': '#FFFF00',
                            'Green': '#00FF00',
                            'Blue': '#0000FF'
                        };
                        colorCode = colorMap[colorParam.value] || null;
                    }
                }
            }
            
            // Extract certificate metadata from signature
            let signature = undefined;
            const certSection = parsed.alert?.Signature?.KeyInfo?.X509Data?.X509Certificate;
            if (certSection) {
                try {
                    // Clean up the certificate data by removing HTML entities and whitespace
                    const cleanCert = certSection.replace(/&#13;/g, '').replace(/\s/g, '');
                    const certData = atob(cleanCert);
                    
                    // Generate SHA-256 fingerprint (more secure than SHA-1)
                    const sha256Hash = createHash('sha256');
                    sha256Hash.update(cleanCert, 'base64');
                    const fingerprintHex = sha256Hash.digest('hex').toUpperCase();
                    const fingerprint = fingerprintHex.match(/.{2}/g)?.join(':') || fingerprintHex;
                    
                    // Extract certificate info using more flexible patterns
                    const cnMatch = certData.match(/CN=([^,]+)/);
                    const oMatch = certData.match(/O=([^,]+)/);
                    // Look for validity period in ASN.1 format - get the second date (expiration)
                    const dateMatches = certData.match(/(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})Z/g);
                    const validMatch = dateMatches && dateMatches.length >= 2 ? 
                        dateMatches[1].match(/(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})Z/) : null;
                    
                    signature = {
                        issuer: cnMatch ? cnMatch[1].trim() : 'MetService',
                        subject: oMatch ? oMatch[1].trim() : Task.CERT_DEFAULTS.SUBJECT,
                        validUntil: validMatch ? `20${validMatch[1]}-${validMatch[2]}-${validMatch[3]}` : Task.CERT_DEFAULTS.VALID_UNTIL,
                        fingerprint: fingerprint || 'Unknown'
                    };
                } catch (error) {
                    console.error('Certificate parsing error:', error instanceof Error ? error.message : 'Unknown error');
                    // Fallback signature info if parsing fails
                    signature = {
                        issuer: Task.CERT_DEFAULTS.ISSUER,
                        subject: Task.CERT_DEFAULTS.SUBJECT,
                        validUntil: Task.CERT_DEFAULTS.VALID_UNTIL,
                        fingerprint: 'Unknown'
                    };
                }
            }
            
            return {
                identifier,
                sender,
                sent,
                status,
                msgType,
                scope,
                info: {
                    category,
                    event,
                    urgency,
                    severity,
                    certainty,
                    senderName,
                    headline,
                    description,
                    instruction,
                    responseType,
                    onset,
                    expires,
                    web,
                    area: {
                        areaDesc,
                        polygon,
                        circle
                    },
                    colorCode
                },
                signature
            };
        } catch (error) {
            console.error('Error parsing CAP XML:', error);
            return null;
        }
    }

    private isCapAlertLink(link: string): boolean {
        return link.includes('/cap/') || link.includes('alert');
    }

    private async parseFeed(feedText: string): Promise<string[]> {
        const links: string[] = [];
        
        // RSS format - use matchAll for safer execution with length limits to prevent ReDoS
        const rssMatches = feedText.matchAll(/<link>([^<]{1,1000})<\/link>/g);
        for (const match of rssMatches) {
            const link = match[1].trim();
            if (this.isCapAlertLink(link)) {
                links.push(link);
            }
        }
        
        // Atom format - use matchAll for safer execution with length limits to prevent ReDoS
        const atomMatches = feedText.matchAll(/<link[^>]+href=["']([^"']{1,1000})["'][^>]*>/g);
        for (const match of atomMatches) {
            const link = match[1].trim();
            if (this.isCapAlertLink(link)) {
                links.push(link);
            }
        }
        
        return [...new Set(links)];
    }

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Environment
            } else {
                return Type.Object({})
            }
        } else {
            return Type.Object({})
        }
    }

    async control(): Promise<void> {
        const env = await this.env(Environment);

        const headers: Record<string, string> = {};
        for (const header of env.Headers || []) {
            headers[header.key] = header.value;
        }

        // Fetch RSS/Atom feed
        const feedUrl = new URL(env.RSS_URL);
        const feedRes = await this.fetchWithRetry(feedUrl, headers, env.Timeout, env.Retries);
        const feedText = await feedRes.text();
        
        // Parse feed to get CAP alert URLs
        const alertUrls = await this.parseFeed(feedText);
        console.log(`Found ${alertUrls.length} CAP alerts in feed`);

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        // Fetch and process each CAP alert
        for (const alertUrl of alertUrls) {
            try {
                const capRes = await this.fetchWithRetry(new URL(alertUrl), headers, env.Timeout, env.Retries);
                const capXml = await capRes.text();
                
                const alert = await this.parseXML(capXml);
                if (!alert) {
                    continue;
                }

                // Create feature from CAP alert
                let geometry: SupportedGeometry | null = null;
                
                if (alert.info.area.polygon) {
                    try {
                        // Handle both single polygon string and array of polygon strings
                        const polygons = Array.isArray(alert.info.area.polygon) ? alert.info.area.polygon : [alert.info.area.polygon];
                        
                        console.log(`Processing ${polygons.length} polygon(s) for alert ${alert.identifier}`);
                        
                        // Process each polygon separately
                        for (let i = 0; i < polygons.length; i++) {
                            console.log(`Processing polygon ${i + 1}/${polygons.length}: ${polygons[i].substring(0, 100)}...`);
                            const coordinates = this.parsePolygonString(polygons[i]);
                            
                            if (coordinates[0].length >= 4) {
                                const polygonGeometry = {
                                    type: 'Polygon' as const,
                                    coordinates
                                };
                                
                                const polygonId = polygons.length > 1 ? `${alert.identifier}-${i}` : alert.identifier;
                                
                                const polygonFeature = {
                                    id: polygonId,
                                    type: 'Feature' as const,
                                    properties: {
                                        callsign: alert.info.headline || 'CAP Alert',
                                        type: 'a-f-X-i',
                                        time: new Date(alert.sent).toISOString(),
                                        start: alert.info.onset ? new Date(alert.info.onset).toISOString() : new Date(alert.sent).toISOString(),
                                        stale: alert.info.expires ? new Date(alert.info.expires).toISOString() : undefined,
                                        metadata: {
                                            sender: alert.sender,
                                            sent: alert.sent,
                                            status: alert.status,
                                            msgType: alert.msgType,
                                            scope: alert.scope,
                                            category: alert.info.category,
                                            event: alert.info.event,
                                            urgency: alert.info.urgency,
                                            severity: alert.info.severity,
                                            certainty: alert.info.certainty,
                                            senderName: alert.info.senderName,
                                            headline: alert.info.headline,
                                            description: alert.info.description,
                                            instruction: alert.info.instruction,
                                            responseType: alert.info.responseType,
                                            onset: alert.info.onset,
                                            expires: alert.info.expires,
                                            web: alert.info.web,
                                            areaDesc: alert.info.area.areaDesc
                                        },
                                        remarks: [
                                            'Description: ' + alert.info.description || '',
                                            'Instruction: ' + alert.info.instruction || '',
                                            'Category: ' + this.getCategoryDescription(alert.info.category),
                                            'Event: ' + this.getEventDescription(alert.info.event),
                                            'Urgency: ' + (alert.info.urgency || 'Unknown'),
                                            'Severity: ' + (alert.info.severity || 'Unknown'),
                                            'Certainty: ' + (alert.info.certainty || 'Unknown'),
                                            'Response: ' + (alert.info.responseType || 'Unknown'),
                                            ...(alert.info.onset ? ['Onset: ' + new Date(alert.info.onset).toLocaleString('en-NZ', { timeZone: 'Pacific/Auckland' }) + ' NZT'] : []),
                                            ...(alert.info.expires ? ['Expires: ' + new Date(alert.info.expires).toLocaleString('en-NZ', { timeZone: 'Pacific/Auckland' }) + ' NZT'] : []),
                                            ...(alert.signature ? [
                                                '',
                                                'Digital Signature',
                                                'Name: ' + (alert.signature.subject || 'Unknown'),
                                                'Issuer: ' + (alert.signature.issuer || 'Unknown'),
                                                'Valid Until: ' + (alert.signature.validUntil || 'Unknown'),
                                                'Fingerprint: ' + (alert.signature.fingerprint || 'Unknown')
                                            ] : [])
                                        ].filter(r => r.trim()).join('\n'),
                                        ...(alert.info.web ? {
                                            links: [{
                                                uid: polygonId,
                                                relation: 'r-u',
                                                mime: 'text/html',
                                                url: alert.info.web,
                                                remarks: 'CAP Alert Details'
                                            }]
                                        } : {}),
                                        ...(alert.info.colorCode ? {
                                            stroke: alert.info.colorCode,
                                            'stroke-opacity': 0.5,
                                            'stroke-width': 3,
                                            'stroke-style': 'solid',
                                            'fill-opacity': 0.4,
                                            fill: alert.info.colorCode
                                        } : {}),
                                        archived: false
                                    },
                                    geometry: polygonGeometry
                                };
                                
                                console.log(`Adding polygon feature: ${polygonId} - ${alert.info.headline}`);
                                fc.features.push(polygonFeature);
                                
                                // Add center point with icon
                                const centroid = this.calculatePolygonCentroid(coordinates);
                                console.log(`Adding center point for ${polygonId} at [${centroid[0]}, ${centroid[1]}]`);
                                const centerFeature = {
                                    id: `${polygonId}-center`,
                                    type: 'Feature' as const,
                                    properties: {
                                        callsign: alert.info.headline || 'CAP Alert',
                                        type: 'a-f-X-i',
                                        time: new Date(alert.sent).toISOString(),
                                        start: alert.info.onset ? new Date(alert.info.onset).toISOString() : new Date(alert.sent).toISOString(),
                                        stale: alert.info.expires ? new Date(alert.info.expires).toISOString() : undefined,
                                        icon: this.getEventIcon(alert.info.event, alert.info.category),
                                        metadata: {
                                            ...polygonFeature.properties.metadata,
                                            isCenter: true
                                        },
                                        remarks: polygonFeature.properties.remarks,
                                        ...(alert.info.web ? {
                                            links: [{
                                                uid: `${polygonId}-center`,
                                                relation: 'r-u',
                                                mime: 'text/html',
                                                url: alert.info.web,
                                                remarks: 'CAP Alert Details'
                                            }]
                                        } : {}),
                                        archived: false
                                    },
                                    geometry: {
                                        type: 'Point' as const,
                                        coordinates: centroid
                                    }
                                };
                                fc.features.push(centerFeature);
                            }
                        }
                        continue; // Skip the rest of the processing for this alert
                    } catch (error) {
                        console.warn(`Invalid polygon data for alert ${alert.identifier}:`, error instanceof Error ? error.message : 'Unknown error');
                    }
                } else if (alert.info.area.circle) {
                    const circleData = this.parseCircleString(alert.info.area.circle);
                    if (circleData) {
                        geometry = {
                            type: 'Point',
                            coordinates: circleData.center
                        };
                    }
                }

                // If no geometry, create a point at center of NZ as fallback
                if (!geometry) {
                    geometry = {
                        type: 'Point',
                        coordinates: [174.0, -41.0] // Approximate center of New Zealand
                    };
                }

                // Handle circle or point geometry
                const baseStyle = alert.info.colorCode ? {
                    stroke: alert.info.colorCode,
                    'stroke-opacity': 0.5019607843137255,
                    'stroke-width': 3,
                    'stroke-style': 'solid',
                    'fill-opacity': 0.5019607843137255,
                    fill: alert.info.colorCode
                } : {};

                const feature = {
                    id: alert.identifier,
                    type: 'Feature' as const,
                    properties: {
                        callsign: alert.info.headline || 'CAP Alert',
                        type: 'a-f-X-i',
                        time: new Date(alert.sent).toISOString(),
                        start: alert.info.onset ? new Date(alert.info.onset).toISOString() : new Date(alert.sent).toISOString(),
                        stale: alert.info.expires ? new Date(alert.info.expires).toISOString() : undefined,
                        icon: this.getEventIcon(alert.info.event, alert.info.category),
                        metadata: {
                            sender: alert.sender,
                            sent: alert.sent,
                            status: alert.status,
                            msgType: alert.msgType,
                            scope: alert.scope,
                            category: alert.info.category,
                            event: alert.info.event,
                            urgency: alert.info.urgency,
                            severity: alert.info.severity,
                            certainty: alert.info.certainty,
                            senderName: alert.info.senderName,
                            headline: alert.info.headline,
                            description: alert.info.description,
                            instruction: alert.info.instruction,
                            responseType: alert.info.responseType,
                            onset: alert.info.onset,
                            expires: alert.info.expires,
                            web: alert.info.web,
                            areaDesc: alert.info.area.areaDesc
                        },
                        remarks: [
                            'Description: ' + alert.info.description || '',
                            'Instruction: ' + alert.info.instruction || '',
                            'Category: ' + this.getCategoryDescription(alert.info.category),
                            'Event: ' + this.getEventDescription(alert.info.event),
                            'Urgency: ' + (alert.info.urgency || 'Unknown'),
                            'Severity: ' + (alert.info.severity || 'Unknown'),
                            'Certainty: ' + (alert.info.certainty || 'Unknown'),
                            'Response: ' + (alert.info.responseType || 'Unknown'),
                            ...(alert.info.onset ? ['Onset: ' + new Date(alert.info.onset).toLocaleString('en-NZ', { timeZone: 'Pacific/Auckland' }) + ' NZT'] : []),
                            ...(alert.info.expires ? ['Expires: ' + new Date(alert.info.expires).toLocaleString('en-NZ', { timeZone: 'Pacific/Auckland' }) + ' NZT'] : []),
                            ...(alert.signature ? [
                                '',
                                'Digital Signature',
                                'Name: ' + (alert.signature.subject || 'Unknown'),
                                'Issuer: ' + (alert.signature.issuer || 'Unknown'),
                                'Valid Until: ' + (alert.signature.validUntil || 'Unknown'),
                                'Fingerprint: ' + (alert.signature.fingerprint || 'Unknown')
                            ] : [])
                        ].filter(r => r.trim()).join('\n'),
                        ...(alert.info.web ? {
                            links: [{
                                uid: alert.identifier,
                                relation: 'r-u',
                                mime: 'text/html',
                                url: alert.info.web,
                                remarks: 'CAP Alert Details'
                            }]
                        } : {}),
                        style: baseStyle,
                        archived: false
                    },
                    geometry
                };

                console.log(`Adding feature: ${alert.identifier} (${geometry.type}) - ${alert.info.headline}`);
                fc.features.push(feature);
            } catch (error) {
                console.error(`Error processing CAP alert ${alertUrl}:`, error);
            }
        }

        console.log(`ok - obtained ${fc.features.length} CAP-NZ features`);
        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

