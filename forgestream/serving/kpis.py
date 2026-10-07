"""
Operational KPI Computation Engine for ForgeStream Phase 4.
Calculates project-defined operational and predictive maintenance KPIs
from live asset states, predictions, telemetry windows, and alert streams.

Note on Standards:
All thresholds and formulas defined herein are ForgeStream Analytical KPIs
and project policy parameters designed for academic demonstration and operational
monitoring, not universal industrial standards.
"""

from typing import Any, Dict, List, Optional
import numpy as np
from forgestream.serving.schemas import (
    AssetCurrentState,
    FleetHealthSummary,
    HealthStateEnum,
    MaintenancePriorityEnum,
    MaintenancePriorityItem,
)


class OperationalKPIEngine:
    """
    Computes mathematical operational KPIs and prescriptive rankings across
    the monitored industrial asset fleet.
    """

    # Project policy parameters
    HIGH_RISK_THRESHOLD: float = 0.50  # Probability threshold for high failure risk
    NEAR_FAILURE_RUL_HOURS: float = 24.0  # RUL threshold (hours) for near-failure warning
    MAX_RUL_HOURS: float = 120.0  # Maximum theoretical RUL horizon (Phase 3 contract)

    # Asset structural criticality weights
    CRITICALITY_WEIGHTS: Dict[str, float] = {
        "LOW": 0.1,
        "MEDIUM": 0.25,
        "HIGH": 0.4,
        "CRITICAL": 0.5,
    }

    @classmethod
    def compute_fleet_summary(
        cls,
        asset_states: List[AssetCurrentState],
        active_alert_count: int = 0,
    ) -> FleetHealthSummary:
        """
        Computes the fleet-wide health summary and aggregated operational KPIs.

        Formulas:
          - Fleet Health Score = (1 / N) * sum(H_i)
          - Critical Asset Count = sum(I(HealthState == CRITICAL))
          - High Risk Asset Count = sum(I(P_fail >= 0.50))
          - Average Predicted RUL = (1 / N) * sum(RUL_i)
          - Failure Risk Rate = High Risk Assets / Total Assets
          - Assets Near Failure = sum(I(RUL < 24.0h))
        """
        if not asset_states:
            return FleetHealthSummary(
                total_assets=0,
                healthy_assets=0,
                watch_assets=0,
                degraded_assets=0,
                critical_assets=0,
                high_risk_assets=0,
                near_failure_assets=0,
                fleet_health_score=1.0,
                avg_predicted_rul_hours=cls.MAX_RUL_HOURS,
                failure_risk_rate=0.0,
                active_alerts_total=active_alert_count,
                emergency_maintenance_count=0,
                high_maintenance_count=0,
            )

        n = len(asset_states)
        healthy = sum(1 for a in asset_states if a.health_state == HealthStateEnum.HEALTHY)
        watch = sum(1 for a in asset_states if a.health_state == HealthStateEnum.WATCH)
        degraded = sum(1 for a in asset_states if a.health_state == HealthStateEnum.DEGRADED)
        critical = sum(1 for a in asset_states if a.health_state == HealthStateEnum.CRITICAL)

        high_risk = sum(1 for a in asset_states if a.failure_probability >= cls.HIGH_RISK_THRESHOLD)
        near_failure = sum(1 for a in asset_states if a.predicted_rul_hours < cls.NEAR_FAILURE_RUL_HOURS)

        emergency_maint = sum(1 for a in asset_states if a.maintenance_priority == MaintenancePriorityEnum.EMERGENCY)
        high_maint = sum(1 for a in asset_states if a.maintenance_priority == MaintenancePriorityEnum.HIGH)

        avg_health = float(np.mean([a.health_score for a in asset_states]))
        avg_rul = float(np.mean([a.predicted_rul_hours for a in asset_states]))
        min_rul = float(np.min([a.predicted_rul_hours for a in asset_states]))
        risk_rate = float(high_risk / n) if n > 0 else 0.0

        total_alerts = active_alert_count + sum(a.active_alert_count for a in asset_states)

        return FleetHealthSummary(
            total_assets=n,
            healthy_assets=healthy,
            watch_assets=watch,
            degraded_assets=degraded,
            critical_assets=critical,
            high_risk_assets=high_risk,
            near_failure_assets=near_failure,
            fleet_health_score=round(avg_health, 4),
            avg_predicted_rul_hours=round(avg_rul, 2),
            min_predicted_rul_hours=round(min_rul, 2),
            failure_risk_rate=round(risk_rate, 4),
            active_alerts_total=total_alerts,
            emergency_maintenance_count=emergency_maint,
            high_maintenance_count=high_maint,
        )

    @classmethod
    def compute_maintenance_priority_score(
        cls,
        health_score: float,
        failure_probability: float,
        predicted_rul_hours: float,
        criticality: str = "MEDIUM",
    ) -> float:
        """
        Computes a composite priority index score S_priority in [0.0, 100.0]
        used to rank assets requiring intervention.

        Formula:
          S_priority = 100 * (
              0.35 * (1.0 - health_score) +
              0.35 * failure_probability +
              0.20 * max(0.0, 1.0 - (predicted_rul_hours / MAX_RUL_HOURS)) +
              0.10 * criticality_weight
          )
        """
        h_comp = max(0.0, min(1.0, 1.0 - health_score))
        p_comp = max(0.0, min(1.0, failure_probability))
        rul_comp = max(0.0, min(1.0, 1.0 - (predicted_rul_hours / cls.MAX_RUL_HOURS)))
        crit_weight = cls.CRITICALITY_WEIGHTS.get(criticality.upper(), 0.25)

        raw_score = 100.0 * (0.35 * h_comp + 0.35 * p_comp + 0.20 * rul_comp + 0.10 * crit_weight)
        return round(float(np.clip(raw_score, 0.0, 100.0)), 2)

    @classmethod
    def derive_recommended_action(
        cls,
        health_state: HealthStateEnum,
        failure_prob: float,
        rul_hours: float,
        priority: MaintenancePriorityEnum,
    ) -> str:
        """Derives explainable prescriptive maintenance recommendations."""
        if priority == MaintenancePriorityEnum.EMERGENCY or health_state == HealthStateEnum.CRITICAL:
            return f"IMMEDIATE SHUTDOWN & INSPECTION: Failure imminent within {rul_hours:.1f}h (Risk: {failure_prob*100:.1f}%)"
        elif priority == MaintenancePriorityEnum.HIGH or failure_prob >= 0.50:
            return f"SCHEDULE EXPEDITED OVERHAUL: Component degradation detected (RUL: {rul_hours:.1f}h)"
        elif priority == MaintenancePriorityEnum.MEDIUM or health_state == HealthStateEnum.DEGRADED:
            return f"INSPECT WITHIN 48H: Early anomaly progression observed (Health: {health_state.value})"
        elif health_state == HealthStateEnum.WATCH:
            return "INCREASE TELEMETRY SAMPLING: Minor variance from baseline profile"
        else:
            return "NORMAL OPERATION: Continue routine scheduled lubrication and monitoring"

    @classmethod
    def rank_maintenance_work_orders(
        cls,
        asset_states: List[AssetCurrentState],
        asset_criticalities: Optional[Dict[str, str]] = None,
    ) -> List[MaintenancePriorityItem]:
        """
        Ranks all assets by composite maintenance urgency score descending.
        """
        criticality_map = asset_criticalities or {}
        items: List[MaintenancePriorityItem] = []

        for asset in asset_states:
            crit = criticality_map.get(asset.asset_id, "MEDIUM")
            p_score = cls.compute_maintenance_priority_score(
                health_score=asset.health_score,
                failure_probability=asset.failure_probability,
                predicted_rul_hours=asset.predicted_rul_hours,
                criticality=crit,
            )
            action = cls.derive_recommended_action(
                health_state=asset.health_state,
                failure_prob=asset.failure_probability,
                rul_hours=asset.predicted_rul_hours,
                priority=asset.maintenance_priority,
            )
            items.append(
                MaintenancePriorityItem(
                    ranking=0,  # Assigned after sorting
                    asset_id=asset.asset_id,
                    asset_type=asset.asset_type,
                    criticality=crit,
                    health_state=asset.health_state,
                    health_score=asset.health_score,
                    failure_probability=asset.failure_probability,
                    predicted_rul_hours=asset.predicted_rul_hours,
                    maintenance_priority=asset.maintenance_priority,
                    priority_score=p_score,
                    recommended_action=action,
                    last_evaluated=asset.last_prediction_time,
                )
            )

        # Sort descending by priority_score
        items.sort(key=lambda x: x.priority_score, reverse=True)
        for rank, item in enumerate(items, start=1):
            item.ranking = rank

        return items
