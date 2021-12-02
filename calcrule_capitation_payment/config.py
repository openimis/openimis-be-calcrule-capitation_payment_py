CLASS_RULE_PARAM_VALIDATION = []

FROM_TO = [
        {"from": "BatchRun", "to": "Bill"},
        {"from": "CapitationPayment", "to": "BillItem"}
]

DESCRIPTION_CONTRIBUTION_VALUATION = F"" \
    F"This calculation will, for the selected level and product," \
    F" calculate how much the insurance need to" \
    F" the HF for the capitation financing"
