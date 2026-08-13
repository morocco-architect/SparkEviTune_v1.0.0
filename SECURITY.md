# Security policy

Do not submit Spark event logs containing credentials, patient data, customer data or other regulated content. SparkEviTune filters configuration keys that look like secrets, but this prototype is not a data-loss-prevention system.

Report security issues privately to the repository maintainers. Do not open a public issue containing credentials or raw production logs.

The software never auto-applies a configuration. Preserve this default unless deployment actions are isolated behind explicit authentication, authorization, audit logging and human approval.
