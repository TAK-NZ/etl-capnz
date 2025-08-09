
import { Static, Type, TSchema } from '@sinclair/typebox';
import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType, InputFeatureCollection } from '@tak-ps/etl';
import { XMLParser } from 'fast-xml-parser';

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
            polygon?: string;
            circle?: string;
        };
        colorCode?: string;
    };
    signature?: {
        issuer?: string;
        subject?: string;
        validUntil?: string;
    };
}

export default class Task extends ETL {
    static name = 'etl-capnz';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    private getCategoryDescription(categoryCode: string): string {
        const categoryMap: Record<string, string> = {
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
        
        return categoryMap[categoryCode] || categoryCode || 'Unknown';
    }

    private getEventDescription(eventCode: string): string {
        const eventMap: Record<string, string> = {
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
            'Tsunami': 'Tsunami',
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
        
        return eventMap[eventCode] || eventCode || 'Unknown';
    }

    private getEventIcon(eventType: string): string {
        const iconMap: Record<string, string> = {
            'snow': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Blizzard-Warning.png',
            'snowfall': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Winter-Storm-Warning.png',
            'rain': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Flood-Warning.png',
            'rainfall': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Flash-Flood-Warning.png',
            'wind': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/High-Wind-Warning.png',
            'storm': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Severe-Weather-Statement.png',
            'thunderstorm': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Severe-Thunderstorm-Warning.png',
            'tornado': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Tornado-Warning.png',
            'tropCyclone': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Hurricane-Warning.png',
            'tropStorm': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Tropical-Storm-Warning.png',
            'flood': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Flood-Warning.png',
            'flashFlood': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Flash-Flood-Warning.png',
            'earthquake': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Earthquake-Warning.png',
            'tsunami': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Tsunami-Warning.png',
            'marine': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Special-Marine-Warning.png',
            'fire': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Fire-Warning.png',
            'fireWeather': 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Fire-Warning.png'
        };
        
        return iconMap[eventType] || 'de450cbf-2ffc-47fb-bd2b-ba2db89b035e:Public Alert/Emergency-Action-Notification-National-Warning.png';
    }

    private parsePolygonString(polygonStr: string): number[][][] {
        const coordPairs = polygonStr.trim().split(' ');
        const points: number[][] = [];
        
        for (const pair of coordPairs) {
            const [latStr, lonStr] = pair.split(',');
            if (latStr && lonStr) {
                const lat = parseFloat(latStr);
                const lon = parseFloat(lonStr);
                if (!isNaN(lat) && !isNaN(lon)) {
                    points.push([lon, lat]); // GeoJSON uses [lon, lat]
                }
            }
        }
        
        // Ensure polygon is closed
        if (points.length > 0 && (points[0][0] !== points[points.length - 1][0] || points[0][1] !== points[points.length - 1][1])) {
            points.push([...points[0]]);
        }
        
        return [points];
    }

    private parseCircleString(circleStr: string): { center: number[]; radius: number } | null {
        const parts = circleStr.trim().split(' ');
        if (parts.length >= 2) {
            const [latStr, lonStr] = parts[0].split(',');
            const radius = parseFloat(parts[1]);
            if (latStr && lonStr) {
                const lat = parseFloat(latStr);
                const lon = parseFloat(lonStr);
                if (!isNaN(lat) && !isNaN(lon) && !isNaN(radius)) {
                    return { center: [lon, lat], radius };
                }
            }
        }
        return null;
    }

    private calculatePolygonCentroid(coordinates: number[][][]): number[] {
        const points = coordinates[0];
        let x = 0, y = 0;
        for (const point of points) {
            x += point[0];
            y += point[1];
        }
        return [x / points.length, y / points.length];
    }

    private async fetchWithRetry(url: URL, headers: Record<string, string>, timeout: number, retries: number): Promise<Response> {
        for (let attempt = 0; attempt <= retries; attempt++) {
            try {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), timeout);
                
                const res = await fetch(url, {
                    method: 'GET',
                    headers,
                    signal: controller.signal
                });
                
                clearTimeout(timeoutId);
                
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status}: ${res.statusText}`);
                }
                
                return res;
            } catch (error) {
                if (attempt === retries) throw error;
                await new Promise(resolve => setTimeout(resolve, 1000 * (attempt + 1)));
            }
        }
        throw new Error('All retry attempts failed');
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
                const colorParam = params.find((p: { valueName: string; value: string }) => p.valueName === 'ColourCodeHex');
                colorCode = colorParam?.value || null;
            }
            
            // Extract certificate metadata from signature
            let signature = undefined;
            const certSection = alert.Signature?.KeyInfo?.X509Data?.X509Certificate;
            if (certSection) {
                try {
                    const certData = atob(certSection.replace(/\s/g, ''));
                    const issuerMatch = certData.match(/CN=([^,]+)/);
                    const subjectMatch = certData.match(/O=([^,]+)/);
                    const validMatch = certData.match(/25(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})Z/);
                    
                    signature = {
                        issuer: issuerMatch ? issuerMatch[1] : undefined,
                        subject: subjectMatch ? subjectMatch[1] : undefined,
                        validUntil: validMatch ? `20${validMatch[1]}-${validMatch[2]}-${validMatch[3]}` : undefined
                    };
                } catch {
                    // Certificate parsing failed, continue without signature info
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

    private async parseFeed(feedText: string): Promise<string[]> {
        const links: string[] = [];
        
        // RSS format
        const rssLinkRegex = /<link>([^<]+)<\/link>/g;
        let match;
        while ((match = rssLinkRegex.exec(feedText)) !== null) {
            const link = match[1].trim();
            if (link.includes('/cap/') || link.includes('alert')) {
                links.push(link);
            }
        }
        
        // Atom format
        const atomLinkRegex = /<link[^>]+href=["']([^"']+)["'][^>]*>/g;
        while ((match = atomLinkRegex.exec(feedText)) !== null) {
            const link = match[1].trim();
            if (link.includes('/cap/') || link.includes('alert')) {
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
                    const coordinates = this.parsePolygonString(alert.info.area.polygon);
                    if (coordinates[0].length >= 4) {
                        geometry = {
                            type: 'Polygon',
                            coordinates
                        };
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
                        ...(geometry.type === 'Point' ? {
                            icon: this.getEventIcon(alert.info.event)
                        } : {}),
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
                            'Icon: ' + this.getEventIcon(alert.info.event),
                            ...(alert.signature ? [
                                'Cert: ' + (alert.signature.subject || 'Unknown'),
                                'Issuer: ' + (alert.signature.issuer || 'Unknown'),
                                'Valid Until: ' + (alert.signature.validUntil || 'Unknown')
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

                fc.features.push(feature);

                // Add center point with icon for polygons
                if (geometry.type === 'Polygon') {
                    const centroid = this.calculatePolygonCentroid(geometry.coordinates);
                    const centerFeature = {
                        id: `${alert.identifier}-center`,
                        type: 'Feature' as const,
                        properties: {
                            callsign: alert.info.headline || 'CAP Alert',
                            type: 'a-f-X-i',
                            time: new Date(alert.sent).toISOString(),
                            start: alert.info.onset ? new Date(alert.info.onset).toISOString() : new Date(alert.sent).toISOString(),
                            stale: alert.info.expires ? new Date(alert.info.expires).toISOString() : undefined,
                            icon: this.getEventIcon(alert.info.event),
                            metadata: {
                                ...feature.properties.metadata,
                                isCenter: true
                            },
                            remarks: feature.properties.remarks,
                            ...(alert.info.web ? {
                                links: [{
                                    uid: `${alert.identifier}-center`,
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

