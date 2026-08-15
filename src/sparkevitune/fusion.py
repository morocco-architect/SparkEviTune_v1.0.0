from __future__ import annotations

from .models import CandidateConfiguration, Recommendation


class RecommendationFusion:
    def fuse(
        self,
        rule_recommendations: list[Recommendation],
        candidate: CandidateConfiguration | None,
        current_config: dict[str, str],
    ) -> list[Recommendation]:
        by_parameter = {rec.parameter: rec for rec in rule_recommendations}
        if candidate is None:
            return list(rule_recommendations)

        predicted_duration = candidate.prediction.duration_s
        for parameter, value in candidate.values.items():
            current = str(current_config.get(parameter, "<unset>"))
            if current == str(value):
                continue
            if parameter in by_parameter:
                rule = by_parameter[parameter]
                if str(rule.recommended_value) == str(value):
                    rule.confidence = min(1.0, rule.confidence + 0.1)
                    rule.evidence.append("ML optimizer independently selected the same value.")
                else:
                    rule.evidence.append(
                        f"ML candidate={value}; deterministic rule={rule.recommended_value}. "
                        "The validated deterministic value remains authoritative until benchmarked."
                    )
                continue
            by_parameter[parameter] = Recommendation(
                source="ml_optimizer",
                symptom="multi_objective_optimization",
                priority="LOW",
                parameter=parameter,
                current_value=current,
                recommended_value=str(value),
                explanation=(
                    "Selected by the surrogate-model optimizer to minimize predicted duration, spill, cost and OOM risk. "
                    "This is an experimental candidate and requires validation by re-running the job."
                ),
                expected_gain=(
                    f"Predicted duration: {predicted_duration:.2f}s"
                    if predicted_duration is not None
                    else "Predicted improvement unavailable."
                ),
                confidence=0.6,
                evidence=[f"optimizer_objective={candidate.objective}"],
            )
        order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        return sorted(by_parameter.values(), key=lambda rec: order.get(rec.priority, 9))
