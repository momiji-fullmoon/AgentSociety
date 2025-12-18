import React, { useContext, useEffect, useState } from 'react';
import DeckGL from '@deck.gl/react';
import { FlyToInterpolator, MapView, MapViewState } from '@deck.gl/core';
import { HeatmapLayer, TextLayer, IconLayer, ScatterplotLayer } from 'deck.gl';
import { Map as MapGL } from 'react-map-gl';
import tinycolor from "tinycolor2";
import { LngLat } from './components/type';
import 'mapbox-gl/dist/mapbox-gl.css';
import { observer } from 'mobx-react-lite';
import { StoreContext } from './store';

// Set your mapbox access token here
const MAPBOX_ACCESS_TOKEN = 'pk.eyJ1IjoiZmh5ZHJhbGlzayIsImEiOiJja3VzMWc5NXkwb3RnMm5sbnVvd3IydGY0In0.FrwFkYIMpLbU83K9rHSe8w';
const MAP_STYLE = 'mapbox://styles/mapbox/standard';

const AOI_COLOR_MAP = new Map<string, string>([
    ['LAND_USE_TYPE_UNSPECIFIED', '#5C8D71'],
    ['LAND_USE_TYPE_COMMERCIAL', '#5B1697'],
    ['LAND_USE_TYPE_INDUSTRIAL', '#82470C'],
    ['LAND_USE_TYPE_RESIDENTIAL', '#fffe00'],
    ['LAND_USE_TYPE_PUBLIC', '#EC3022'],
    ['LAND_USE_TYPE_TRANSPORTATION', '#979B9A'],
    ['LAND_USE_TYPE_OTHER', '#5C8D71'],
]);

const LAND_USE_NAME = new Map<string, string>([
    ['LAND_USE_TYPE_UNSPECIFIED', '未指定'],
    ['LAND_USE_TYPE_COMMERCIAL', '商服用地'],
    ['LAND_USE_TYPE_INDUSTRIAL', '工矿仓储用地'],
    ['LAND_USE_TYPE_RESIDENTIAL', '住宅用地'],
    ['LAND_USE_TYPE_PUBLIC', '公共管理与公共服务用地'],
    ['LAND_USE_TYPE_TRANSPORTATION', '交通运输用地'],
    ['LAND_USE_TYPE_OTHER', '其他土地'],
]);

const BRIDGE_COLORS: Record<string, [number, number, number, number]> = {
    critical: [220, 53, 69, 220],
    inProgress: [255, 165, 0, 220],
    default: [52, 152, 219, 200],
};

const bridgeStatusColor = (priority?: string, workOrderStatus?: string): [number, number, number, number] => {
    const normalizedPriority = priority?.toLowerCase();
    const normalizedStatus = workOrderStatus?.toLowerCase();
    if (normalizedStatus && normalizedStatus.includes('progress')) {
        return BRIDGE_COLORS.inProgress;
    }
    if (normalizedPriority === 'critical') {
        return BRIDGE_COLORS.critical;
    }
    return BRIDGE_COLORS.default;
};


const Deck = observer((props: {
    style: React.CSSProperties,
}) => {
    const store = useContext(StoreContext)

    const [curZoom, setCurZoom] = useState(10.5);
    const [hovering, setHovering] = useState(false);

    let layers = [];

    const bridgeOverlays = store.bridgeOverlays ?? [];

    // const aoiLayers = props.showAoi ? [new GeoJsonLayer({
    //     id: 'aoi',
    //     data: HTTPBackendUrl + `/experiments/${store.expID}/geojson/aoi`,
    //     loadOptions: {
    //         fetch: {
    //             mode: 'cors',
    //             credentials: 'include',
    //         },
    //     },
    //     pickable: true,
    //     stroked: false, // 控制所有元素是否描边
    //     filled: true, // 控制Polygon是否填充
    //     // extruded: true, // 控制Polygon是否向Z轴拉伸，形成立体效果，高度来自getElevation
    //     // getElevation: 10,
    //     getFillColor: f => {
    //         const hex = AOI_COLOR_MAP.get(f.properties!.land_use) ?? '#5C8D71';
    //         const rgba = tinycolor(hex).setAlpha(0.4).toRgb();
    //         return [rgba.r, rgba.g, rgba.b, rgba.a * 255];
    //     },
    // })] : [];

    const agentList = Array.from(store.agents.values());

    if (curZoom > 10) {
        const iconLayer = new IconLayer({
            id: 'icon',
            data: agentList.map((a) => {
                const profile = a.profile;
                let avatarUrl = '/icon/agent.png';
                try {
                    if (profile !== undefined) {
                        const gender = profile.gender;
                        const age = profile.age;
                        if (gender === 'male' && typeof age === 'number') {
                            if (age < 18) {
                                avatarUrl = '/icon/boy1.png';
                            } else if (age < 65) {
                                avatarUrl = '/icon/boy2.png';
                            } else {
                                avatarUrl = '/icon/boy3.png';
                            }
                        } else if (gender === 'female' && typeof age === 'number') {
                            if (age < 18) {
                                avatarUrl = '/icon/girl1.png';
                            } else if (age < 65) {
                                avatarUrl = '/icon/girl2.png';
                            } else {
                                avatarUrl = '/icon/girl3.png';
                            }
                        }
                    }
                } catch (e) {
                    console.error(e);
                }
                return {
                    id: a.id,
                    coordinate: [a.lng, a.lat],
                    avatarUrl: avatarUrl,
                }
            }),
            pickable: true,
            getIcon: d => ({
                url: d.avatarUrl,
                width: 128,
                height: 128,
                anchorX: 64,
                anchorY: 64,
            }),
            getSize: 30,
            getPosition: d => d.coordinate,
        });
        // if (iconLayers.length > 0) {
        //     console.log("number of agents: ", iconLayers.length);
        // }
        layers.push(iconLayer);

        const textLayer = new TextLayer({
            id: 'text',
            data: agentList.map((a) => {
                if (a.name === "") {
                    return undefined
                } else {
                    return {
                        id: a.id,
                        position: [a.lng, a.lat],
                        text: a.name,
                    }
                }
            }).filter(d => d !== undefined),
            background: true,
            backgroundPadding: [4, 4, 4, 4],
            characterSet: 'auto',
            fontFamily: 'system-ui',
            getText: d => d.text,
            getPosition: d => d.position,
            getSize: 16,
            getBackgroundColor: [0, 0, 0, 128],
            getColor: [255, 255, 255],
            getAngle: 0,
            getPixelOffset: [0, -24],
            getTextAnchor: 'middle',
            getAlignmentBaseline: 'bottom',
            fontSettings: {
                sdf: true,
                radius: 24,
                fontSize: 128,
            },
            maxWidth: 10,
        });
        layers.push(textLayer);
    } else {
        // use point layer
        const pointLayer = new ScatterplotLayer({
            id: 'point',
            data: agentList.map((a) => {
                return {
                    id: a.id,
                    position: [a.lng, a.lat],
                    radius: 10,
                    // #1677FF
                    color: [22, 119, 255],
                }
            }),
            pickable: true,
            radiusScale: 20,
            radiusMinPixels: 1,
            radiusMaxPixels: 100,
            getPosition: d => d.position,
            getRadius: d => d.radius,
            getFillColor: d => d.color,
        });
        layers.push(pointLayer);
    }

    if (store.heatmapKeyInStatus !== undefined) {
        const heatmapLayer = new HeatmapLayer({
            id: 'heatmap',
            data: agentList.map((a) => {
                return {
                    position: [a.lng, a.lat],
                    weight: a.status[store.heatmapKeyInStatus] ?? 0,
                }
            }),
            getPosition: d => d.position,
            getWeight: d => d.weight,
            threshold: 0.05,
            radiusPixels: 100,
            intensity: 1,
        });
        layers = [heatmapLayer, ...layers];
    }

    const bridgePoints = bridgeOverlays.filter(b => b.lng !== undefined && b.lat !== undefined);
    if (bridgePoints.length > 0) {
        const bridgeLayer = new ScatterplotLayer({
            id: 'bridge-status',
            data: bridgePoints,
            pickable: true,
            radiusScale: 20,
            radiusMinPixels: 6,
            radiusMaxPixels: 200,
            getPosition: d => [d.lng, d.lat],
            getRadius: d => (d.priority?.toLowerCase() === 'critical' ? 180 : 140),
            getFillColor: d => bridgeStatusColor(d.priority, d.work_order_status),
            getLineColor: [0, 0, 0, 120],
            stroked: true,
            lineWidthMinPixels: 1,
        });
        const bridgeLabelLayer = new TextLayer({
            id: 'bridge-status-label',
            data: bridgePoints,
            getText: d => d.name ?? d.bridge_id,
            getPosition: d => [d.lng, d.lat],
            getSize: 12,
            getColor: [40, 40, 40, 255],
            getBackgroundColor: [255, 255, 255, 220],
            getPixelOffset: [0, 16],
            background: true,
            pickable: true,
        });
        layers = [...layers, bridgeLayer, bridgeLabelLayer];
    }

    const mapCenter = store.mapCenter;

    return <div style={props.style} onContextMenu={evt => evt.preventDefault()}>
        <DeckGL
            initialViewState={{
                longitude: mapCenter.lng,
                latitude: mapCenter.lat,
                zoom: 10.5,
                pitch: 0,
                bearing: 0,
                transitionDuration: 2000,
                transitionInterpolator: new FlyToInterpolator(),
            } as MapViewState}
            controller
            layers={layers}
            onViewStateChange={({ viewState }) => {
                const zoom = (viewState as unknown as MapViewState).zoom;
                setCurZoom(zoom);
            }}
            onHover={(info) => {
                const { object, coordinate } = info;
                setHovering(Boolean(object))
            }}
            getCursor={() => hovering ? 'pointer' : 'grab'}
            getTooltip={({ object, layer }) => {
                if (!object || !layer) {
                    return null;
                }
                if (layer.id === 'bridge-status' || layer.id === 'bridge-status-label') {
                    const info = object as any;
                    const header = info.name ?? info.bridge_id;
                    const statusLine = info.work_order_status ? `${info.status ?? 'work_order'} - ${info.work_order_status}` : (info.status ?? 'work_order');
                    const due = info.days_overdue !== undefined ? `Overdue: ${info.days_overdue} days` : (info.due_date ? `Due: ${info.due_date}` : '');
                    const risk = info.priority ? `Priority: ${info.priority}` : '';
                    return {
                        html: `<b>${header}</b><br/>${statusLine}<br/>${risk}<br/>${info.action ?? ''}<br/>${due}`,
                        style: {
                            backgroundColor: 'rgba(0, 0, 0, 0.75)',
                            color: 'white',
                            padding: '6px 8px',
                        },
                    };
                }
                if (layer.id === 'aoi') {
                    const name = object.properties?.name;
                    const id = object.id;
                    const landUse = LAND_USE_NAME.get(object.properties?.land_use);
                    if (name === undefined || id === undefined || landUse === undefined) {
                        return null;
                    }
                    return {
                        html: `<b>${object.properties!.name}</b><br/>ID = ${id}<br/>${landUse}`,
                        style: {
                            backgroundColor: 'rgba(0, 0, 0, 0.8)',
                            color: 'white',
                        }
                    }
                }
                return null;
            }}
            onClick={async (info, event) => {
                console.log(info, event);
                if (event.leftButton) {
                    const { layer, object } = info;
                    if (!layer) {
                        return;
                    }
                    if (layer.id === 'icon' || layer.id === 'point' || layer.id === 'text') {
                        const id = object.id;
                        await store.setClickedAgentID(id);
                    }
                } else {
                    // 右键弹出菜单
                    const { coordinate } = info;
                    if (coordinate) {
                        // TODO: show context menu
                    }
                }
            }}
        >
            {/* @ts-ignore */}
            <MapView id="map" width="100%" controller>
                <MapGL mapboxAccessToken={MAPBOX_ACCESS_TOKEN} mapStyle={MAP_STYLE} />
            </MapView>
        </DeckGL>
    </div>;
});

export default Deck;
