from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from ..agent import (
    AgentContext,
    AgentToolbox,
    Block,
    BlockOutput,
    MemoryAttribute,
)
from ..agent.agent import CitizenAgentBase
from ..memory import Memory
from ..message import Message, MessageKind
from ..tools.bridge_tools import (
    BridgeRecord,
    filter_bridges_by_condition,
    load_clean_bridge_records,
    propose_repair_actions,
    schedule_overdue_inspections,
)


class BridgeTaskOutput(BlockOutput):
    """Aggregate output for bridge-related blocks."""

    route_plan: list[dict[str, Any]] | None = None
    inspection_backlog: list[dict[str, Any]] | None = None
    repair_proposals: list[dict[str, Any]] | None = None
    dispatches: list[Message] | None = None


@dataclass
class BridgeProfile:
    """Profile describing bridge-facing agents."""

    role: str
    jurisdiction: str | None = None
    specialization: str | None = None
    crew_contacts: list[int] = field(default_factory=list)


class BridgeAgentContext(AgentContext):
    profile: BridgeProfile = BridgeProfile(role="bridge-agent")
    latest_output: BridgeTaskOutput | None = None


class BridgeRoleBase(CitizenAgentBase):
    """Common base for bridge inspector, scheduler, and crew roles."""

    BlockOutputType = BridgeTaskOutput
    Context = BridgeAgentContext
    StatusAttributes = [
        MemoryAttribute(
            name="bridge_inventory",
            type=list,
            default_or_value=[],
            description="Shared cleaned bridge records available to all bridge agents",
            whether_embedding=True,
        ),
        MemoryAttribute(
            name="assignments",
            type=list,
            default_or_value=[],
            description="Bridge IDs or names assigned to the agent",
        ),
        MemoryAttribute(
            name="profile",
            type=dict,
            default_or_value={},
            description="Operational profile for the bridge role",
        ),
    ]
    description = "Bridge maintenance specialist"

    def __init__(
        self,
        id: int,
        name: str,
        toolbox: AgentToolbox,
        memory: Memory,
        agent_params: Optional[Any] = None,
        blocks: Optional[list[Block]] = None,
        profile: Optional[BridgeProfile] = None,
        bridge_records: Optional[list[BridgeRecord]] = None,
    ) -> None:
        if profile is None:
            profile = BridgeProfile(role="bridge-agent")
        super().__init__(
            id=id,
            name=name,
            toolbox=toolbox,
            memory=memory,
            agent_params=agent_params,
            blocks=blocks,
        )
        self._bridge_profile = profile
        self._bridge_records = bridge_records

    async def init(self):
        await super().init()
        await self.memory.status.update("profile", self._bridge_profile.__dict__)
        if self._bridge_records is None:
            self._bridge_records = load_clean_bridge_records()
        await self.memory.status.update("bridge_inventory", self._bridge_records)

    @property
    def bridge_profile(self) -> BridgeProfile:
        return self._bridge_profile


class InspectionRouteBlock(Block):
    """Plan inspection routes based on risk and overdue dates."""

    OutputType = BridgeTaskOutput
    name = "bridge-inspection-route"
    description = "Filters risky bridges and orders them for inspection"
    actions = {"plan_route": "Compute prioritized inspection route"}
    NeedAgent = True

    async def forward(self, agent_context: BridgeAgentContext):
        records = await self.agent.memory.status.get("bridge_inventory", [])
        risky = filter_bridges_by_condition(records, max_condition_score=4)
        backlog = schedule_overdue_inspections(risky)
        route_plan: list[dict[str, Any]] = []
        for entry in backlog:
            bridge = entry["bridge"]
            route_plan.append(
                {
                    "bridge": bridge,
                    "due_date": entry["due_date"],
                    "priority": "overdue" if entry["days_overdue"] > 0 else "soon",
                }
            )
        output = BridgeTaskOutput(route_plan=route_plan, inspection_backlog=backlog)
        agent_context.latest_output = output
        return output


class ConditionReasoningBlock(Block):
    """Reason about condition codes and propose repairs."""

    OutputType = BridgeTaskOutput
    name = "bridge-condition-reasoner"
    description = "Generates repair proposals from condition scores"
    actions = {"reason_condition": "Propose repairs for inspected bridges"}
    NeedAgent = True

    async def forward(self, agent_context: BridgeAgentContext):
        route_output = agent_context.latest_output
        candidate_records: list[BridgeRecord]
        if route_output and route_output.route_plan:
            candidate_records = [entry["bridge"] for entry in route_output.route_plan]
        else:
            candidate_records = await self.agent.memory.status.get("bridge_inventory", [])
        proposals = propose_repair_actions(candidate_records)
        output = BridgeTaskOutput(
            route_plan=route_output.route_plan if route_output else None,
            inspection_backlog=route_output.inspection_backlog if route_output else None,
            repair_proposals=proposals,
        )
        agent_context.latest_output = output
        return output


class RepairDispatchBlock(Block):
    """Dispatch repair tasks to field crews via the message layer."""

    OutputType = BridgeTaskOutput
    name = "bridge-repair-dispatch"
    description = "Creates repair dispatch messages for field crews"
    actions = {"dispatch": "Send repair tasks to field crews"}
    NeedAgent = True

    async def forward(self, agent_context: BridgeAgentContext):
        proposals = []
        if agent_context.latest_output and agent_context.latest_output.repair_proposals:
            proposals = agent_context.latest_output.repair_proposals
        crew_contacts: list[int] = self.agent.bridge_profile.crew_contacts
        dispatches: list[Message] = []
        for idx, proposal in enumerate(proposals):
            target = crew_contacts[idx % len(crew_contacts)] if crew_contacts else None
            dispatches.append(
                Message(
                    from_id=self.agent.id,
                    to_id=target,
                    day=0,
                    t=0.0,
                    kind=MessageKind.AGENT_CHAT,
                    payload={
                        "type": "repair-request",
                        "bridge": proposal.get("bridge"),
                        "priority": proposal.get("priority"),
                        "action": proposal.get("recommended_action"),
                    },
                )
            )
        output = BridgeTaskOutput(
            route_plan=agent_context.latest_output.route_plan if agent_context.latest_output else None,
            inspection_backlog=agent_context.latest_output.inspection_backlog if agent_context.latest_output else None,
            repair_proposals=proposals,
            dispatches=dispatches,
        )
        agent_context.latest_output = output
        return output


class InspectionSchedulerBlock(Block):
    """Block used by maintenance schedulers to triage inspection backlog."""

    OutputType = BridgeTaskOutput
    name = "bridge-inspection-scheduler"
    description = "Uses overdue dates to schedule inspection visits"
    actions = {"schedule_inspection": "Produce ordered inspection backlog"}
    NeedAgent = True

    async def forward(self, agent_context: BridgeAgentContext):
        records = await self.agent.memory.status.get("bridge_inventory", [])
        backlog = schedule_overdue_inspections(records)
        output = BridgeTaskOutput(inspection_backlog=backlog)
        agent_context.latest_output = output
        return output


class CrewActionBlock(Block):
    """Field crew block that turns dispatches into task lists."""

    OutputType = BridgeTaskOutput
    name = "bridge-crew-action"
    description = "Interprets dispatch messages into crew tasks"
    actions = {"plan_repairs": "Translate dispatch into actionable repairs"}
    NeedAgent = True

    async def forward(self, agent_context: BridgeAgentContext):
        pending_messages = await self.agent.toolbox.messager.fetch_received_messages()
        repair_messages = [msg for msg in pending_messages if msg.payload.get("type") == "repair-request"]
        tasks: list[dict[str, Any]] = []
        for msg in repair_messages:
            tasks.append(
                {
                    "bridge": msg.payload.get("bridge"),
                    "priority": msg.payload.get("priority", "routine"),
                    "action": msg.payload.get("action"),
                    "requested_by": msg.from_id,
                }
            )
        output = BridgeTaskOutput(dispatches=repair_messages, repair_proposals=tasks)
        agent_context.latest_output = output
        return output


class BridgeInspectorAgent(BridgeRoleBase):
    """Inspector agent that plans routes and dispatches repair requests."""

    description = "Bridge inspector that triages risky bridges and alerts crews"

    def __init__(
        self,
        id: int,
        name: str,
        toolbox: AgentToolbox,
        memory: Memory,
        agent_params: Optional[Any] = None,
        blocks: Optional[list[Block]] = None,
        profile: Optional[BridgeProfile] = None,
        bridge_records: Optional[list[BridgeRecord]] = None,
    ) -> None:
        if blocks is None:
            blocks = [
                InspectionRouteBlock(toolbox=toolbox, agent_memory=memory),
                ConditionReasoningBlock(toolbox=toolbox, agent_memory=memory),
                RepairDispatchBlock(toolbox=toolbox, agent_memory=memory),
            ]
        super().__init__(
            id=id,
            name=name,
            toolbox=toolbox,
            memory=memory,
            agent_params=agent_params,
            blocks=blocks,
            profile=profile or BridgeProfile(role="inspector"),
            bridge_records=bridge_records,
        )


class MaintenanceSchedulerAgent(BridgeRoleBase):
    """Scheduler that manages inspection backlogs."""

    description = "Bridge maintenance scheduler prioritizing overdue inspections"

    def __init__(
        self,
        id: int,
        name: str,
        toolbox: AgentToolbox,
        memory: Memory,
        agent_params: Optional[Any] = None,
        blocks: Optional[list[Block]] = None,
        profile: Optional[BridgeProfile] = None,
        bridge_records: Optional[list[BridgeRecord]] = None,
    ) -> None:
        if blocks is None:
            blocks = [InspectionSchedulerBlock(toolbox=toolbox, agent_memory=memory)]
        super().__init__(
            id=id,
            name=name,
            toolbox=toolbox,
            memory=memory,
            agent_params=agent_params,
            blocks=blocks,
            profile=profile or BridgeProfile(role="scheduler"),
            bridge_records=bridge_records,
        )


class FieldCrewAgent(BridgeRoleBase):
    """Field crew that executes repair requests."""

    description = "Bridge field crew converting dispatch messages into actions"

    def __init__(
        self,
        id: int,
        name: str,
        toolbox: AgentToolbox,
        memory: Memory,
        agent_params: Optional[Any] = None,
        blocks: Optional[list[Block]] = None,
        profile: Optional[BridgeProfile] = None,
        bridge_records: Optional[list[BridgeRecord]] = None,
    ) -> None:
        if blocks is None:
            blocks = [CrewActionBlock(toolbox=toolbox, agent_memory=memory)]
        super().__init__(
            id=id,
            name=name,
            toolbox=toolbox,
            memory=memory,
            agent_params=agent_params,
            blocks=blocks,
            profile=profile or BridgeProfile(role="field-crew"),
            bridge_records=bridge_records,
        )


__all__ = [
    "BridgeInspectorAgent",
    "BridgeProfile",
    "BridgeRoleBase",
    "FieldCrewAgent",
    "MaintenanceSchedulerAgent",
    "BridgeTaskOutput",
]
