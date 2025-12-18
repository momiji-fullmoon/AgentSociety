export interface AgentProfile {
    id: number;
    name: string;
    profile?: { [key: string]: string | number };
}

export interface AgentStatus {
    id: number;
    day: number;
    t: number;
    lng: number;
    lat: number;
    parent_id: number;
    action: string;
    status: { [key: string]: string | number } | string;
}

export interface AgentDialog {
    id: number;
    day: number;
    t: number;
    type: 0 | 1 | 2;
    speaker: string;
    content: string;
}

export interface AgentSurvey {
    id: number;
    day: number;
    t: number;
    survey_id: string;
    result: { [key: string]: string | number };
}

export interface Agent extends AgentProfile, AgentStatus { }

export interface Time {
    day: number;
    t: number;
}

export interface LngLat {
    lng: number;
    lat: number;
}

export interface ApiMetric {
    key: string;
    value: number;
    step: number;
}

export interface BridgeOverlay {
    bridge_id: string;
    name?: string;
    priority?: string;
    risk?: string;
    status?: string;
    work_order_status?: string;
    action?: string;
    due_date?: string;
    days_overdue?: number;
    lng?: number;
    lat?: number;
    last_update?: { day?: number; t?: number; step?: number };
}

export interface BridgeTimelineLog {
    bridge_id: string;
    day?: number;
    t?: number;
    priority?: string;
    risk?: string;
    due_date?: string;
    days_overdue?: number;
    action?: string;
    response_steps?: number;
    assigned_to?: number;
}
