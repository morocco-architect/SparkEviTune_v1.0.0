# SparkEviTune safety rules

## Human validation

A generated configuration is a recommendation, not an instruction to deploy automatically. Every change must be reviewed and measured with a controlled re-run.

## Memory feasibility

Executor heap plus executor memory overhead must fit within the memory available to the executor container or worker. Driver and executor limits must be validated independently.

## Sensitive configuration

Passwords, tokens, private keys and cloud access keys must not be stored in analysis reports, feature stores, prompts or audit logs.

## Score interpretation

A rule-compliance score of 100 means that none of the implemented deterministic rules produced an actionable finding. It does not prove optimal runtime, minimum cost or absence of unknown anomalies.
