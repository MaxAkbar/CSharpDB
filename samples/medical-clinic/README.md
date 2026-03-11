# Riverside Health Center

Riverside Health Center is a medical clinic sample covering patients, doctors, appointments, prescriptions, records, and billing. It is useful when you want a business workflow with both operational tables and follow-up financial data.

## Files

- `schema.sql` - schema plus seed data
- `procedures.json` - `GetPatientOutstandingBills`, `MarkBillPaid`

## What It Showcases

- Multi-table joins between patients, doctors, appointments, and billing
- Views for operational dashboards: `upcoming_appointments`, `outstanding_bills`
- Trigger-driven record creation with `trg_record_after_appointment`
- Procedure-backed billing workflows for the Admin UI or API
- A clean example of date-heavy, status-heavy transactional data

## Good Starting Points

- `SELECT * FROM upcoming_appointments ORDER BY appointment_date, appointment_time;`
- `SELECT * FROM outstanding_bills ORDER BY billing_date DESC;`
- `EXEC GetPatientOutstandingBills patientId=9;`
- `EXEC MarkBillPaid billingId=8;`

## Load

```text
csdb> .read samples/medical-clinic/schema.sql
```
