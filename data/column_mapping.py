column_mapping = {
    "Supplier ID": str("Supplier ID"),
    "Supplier Name": "Supplier Name",
    "Supplier Enablement Complete": "Originating MDM Status",
    "On CSP": "Originating CSP STatus",
    "On CSP cXML?": "Current PO or Non PO Vendor",
    "Coupa Status": "Coupa Primary Contact Updated",
    "Supplier Portal Status": "RN Status",
    "RiseNow Status": "RN Status"
}

# Additional columns from list 2 that don't have a direct match in list 1
additional_columns = [
    "Priority (80% of Spend Vendor)",
    "RN Owns",
    "Assigned To",
    "Wave",
    "Original Contact Email (From Apex, starting point)",
    "Original Contact Phone",
    "Final Confirmed Contact (CSP Invite Will Go To)",
    "Final Confirmed Contact Phone",
    "OCM Mail Sent to New Contact",
    "KP Sourcing Contact",
    "Supplier Response Date",
    "Response / Done",
    "No Response",
    "Latest Comment",
    "KP Action",
    "Created"
]
