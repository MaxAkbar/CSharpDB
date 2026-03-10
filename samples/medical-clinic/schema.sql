-- ============================================================
-- Riverside Health Center — Medical Clinic
-- ============================================================
-- A multi-specialty medical clinic with doctors, patients,
-- appointments, prescriptions, medical records, and billing.
-- ============================================================

-- ─── Tables ─────────────────────────────────────────────────

CREATE TABLE departments (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    floor           INTEGER NOT NULL,
    phone_extension TEXT NOT NULL
);

CREATE TABLE doctors (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    specialty       TEXT NOT NULL,
    department_id   INTEGER NOT NULL,
    phone           TEXT,
    license_number  TEXT NOT NULL,
    is_active       INTEGER NOT NULL
);

CREATE TABLE patients (
    id                  INTEGER PRIMARY KEY,
    name                TEXT NOT NULL,
    date_of_birth       TEXT NOT NULL,
    gender              TEXT NOT NULL,
    phone               TEXT,
    email               TEXT,
    insurance_provider  TEXT
);

CREATE TABLE appointments (
    id                  INTEGER PRIMARY KEY,
    patient_id          INTEGER NOT NULL,
    doctor_id           INTEGER NOT NULL,
    appointment_date    TEXT NOT NULL,
    appointment_time    TEXT NOT NULL,
    status              TEXT NOT NULL,
    notes               TEXT
);

CREATE TABLE prescriptions (
    id              INTEGER PRIMARY KEY,
    appointment_id  INTEGER NOT NULL,
    patient_id      INTEGER NOT NULL,
    medication      TEXT NOT NULL,
    dosage          TEXT NOT NULL,
    instructions    TEXT,
    prescribed_date TEXT NOT NULL
);

CREATE TABLE medical_records (
    id              INTEGER PRIMARY KEY,
    patient_id      INTEGER NOT NULL,
    doctor_id       INTEGER NOT NULL,
    visit_date      TEXT NOT NULL,
    diagnosis       TEXT NOT NULL,
    treatment       TEXT,
    follow_up_date  TEXT
);

CREATE TABLE billing (
    id                  INTEGER PRIMARY KEY,
    patient_id          INTEGER NOT NULL,
    appointment_id      INTEGER NOT NULL,
    amount              REAL NOT NULL,
    insurance_covered   REAL NOT NULL,
    patient_owes        REAL NOT NULL,
    status              TEXT NOT NULL,
    billing_date        TEXT NOT NULL
);

-- ─── Departments ────────────────────────────────────────────

INSERT INTO departments VALUES (1, 'General Medicine',  1, '1001');
INSERT INTO departments VALUES (2, 'Cardiology',        2, '2001');
INSERT INTO departments VALUES (3, 'Pediatrics',        1, '1002');
INSERT INTO departments VALUES (4, 'Orthopedics',       2, '2002');
INSERT INTO departments VALUES (5, 'Dermatology',       3, '3001');

-- ─── Doctors ────────────────────────────────────────────────

INSERT INTO doctors VALUES (1, 'Dr. Sarah Mitchell',   'General Practice',     1, '555-1001', 'MD-20145', 1);
INSERT INTO doctors VALUES (2, 'Dr. James Rivera',     'Cardiology',           2, '555-1002', 'MD-18932', 1);
INSERT INTO doctors VALUES (3, 'Dr. Priya Sharma',     'Pediatrics',           3, '555-1003', 'MD-21087', 1);
INSERT INTO doctors VALUES (4, 'Dr. Michael Torres',   'Orthopedic Surgery',   4, '555-1004', 'MD-19456', 1);
INSERT INTO doctors VALUES (5, 'Dr. Lisa Chang',       'Dermatology',          5, '555-1005', 'MD-22301', 1);
INSERT INTO doctors VALUES (6, 'Dr. Robert Okafor',    'Internal Medicine',    1, '555-1006', 'MD-17823', 1);

-- ─── Patients ───────────────────────────────────────────────

INSERT INTO patients VALUES (1,  'Maria Garcia',     '1985-03-12', 'Female', '555-2001', 'maria.g@email.com',    'BlueCross');
INSERT INTO patients VALUES (2,  'Thomas Brown',     '1972-07-28', 'Male',   '555-2002', 'tbrown@email.com',     'Aetna');
INSERT INTO patients VALUES (3,  'Sophia Anderson',  '1990-11-05', 'Female', '555-2003', 'sophia.a@email.com',   'United Health');
INSERT INTO patients VALUES (4,  'William Davis',    '1968-01-19', 'Male',   '555-2004', 'wdavis@email.com',     'Cigna');
INSERT INTO patients VALUES (5,  'Olivia Martinez',  '2015-06-30', 'Female', '555-2005', 'omartinez@email.com',  'BlueCross');
INSERT INTO patients VALUES (6,  'Ethan Wilson',     '1995-09-14', 'Male',   '555-2006', 'ethan.w@email.com',    'Aetna');
INSERT INTO patients VALUES (7,  'Isabella Taylor',  '1988-04-22', 'Female', '555-2007', 'itaylor@email.com',    'Kaiser');
INSERT INTO patients VALUES (8,  'Noah Thompson',    '2018-12-01', 'Male',   '555-2008', 'nthompson@email.com',  'United Health');
INSERT INTO patients VALUES (9,  'Ava Robinson',     '1978-08-17', 'Female', '555-2009', 'ava.r@email.com',      'Cigna');
INSERT INTO patients VALUES (10, 'Liam Clark',       '1960-02-25', 'Male',   '555-2010', 'lclark@email.com',     'Medicare');

-- ─── Appointments ───────────────────────────────────────────

INSERT INTO appointments VALUES (1,  1, 1, '2025-01-10', '09:00', 'completed', 'Annual physical exam');
INSERT INTO appointments VALUES (2,  2, 2, '2025-01-15', '10:30', 'completed', 'Follow-up on blood pressure');
INSERT INTO appointments VALUES (3,  5, 3, '2025-01-20', '14:00', 'completed', 'Childhood vaccination');
INSERT INTO appointments VALUES (4,  4, 2, '2025-02-01', '11:00', 'completed', 'Chest pain evaluation');
INSERT INTO appointments VALUES (5,  3, 1, '2025-02-10', '09:30', 'completed', 'Persistent cough');
INSERT INTO appointments VALUES (6,  6, 4, '2025-02-18', '15:00', 'completed', 'Knee injury assessment');
INSERT INTO appointments VALUES (7,  7, 5, '2025-03-05', '10:00', 'completed', 'Skin rash examination');
INSERT INTO appointments VALUES (8,  9, 1, '2025-03-12', '08:30', 'completed', 'Migraine consultation');
INSERT INTO appointments VALUES (9,  10, 6, '2025-03-20', '11:30', 'completed', 'Diabetes management');
INSERT INTO appointments VALUES (10, 1,  1, '2025-04-15', '09:00', 'scheduled', 'Follow-up checkup');
INSERT INTO appointments VALUES (11, 8,  3, '2025-04-18', '14:30', 'scheduled', 'Wellness visit');
INSERT INTO appointments VALUES (12, 2,  2, '2025-04-22', '10:00', 'scheduled', 'Cardiac stress test');

-- ─── Prescriptions ──────────────────────────────────────────

INSERT INTO prescriptions VALUES (1, 2, 2, 'Lisinopril',     '10mg',  'Take once daily in the morning',     '2025-01-15');
INSERT INTO prescriptions VALUES (2, 4, 4, 'Atorvastatin',   '20mg',  'Take once daily at bedtime',         '2025-02-01');
INSERT INTO prescriptions VALUES (3, 5, 3, 'Amoxicillin',    '500mg', 'Take three times daily for 10 days', '2025-02-10');
INSERT INTO prescriptions VALUES (4, 6, 6, 'Ibuprofen',      '400mg', 'Take as needed for pain, max 3/day', '2025-02-18');
INSERT INTO prescriptions VALUES (5, 7, 7, 'Hydrocortisone', '1%',    'Apply to affected area twice daily',  '2025-03-05');
INSERT INTO prescriptions VALUES (6, 8, 9, 'Sumatriptan',    '50mg',  'Take at onset of migraine',          '2025-03-12');
INSERT INTO prescriptions VALUES (7, 9, 10, 'Metformin',     '500mg', 'Take twice daily with meals',        '2025-03-20');
INSERT INTO prescriptions VALUES (8, 2, 2, 'Aspirin',        '81mg',  'Take once daily',                    '2025-01-15');

-- ─── Medical Records ────────────────────────────────────────

INSERT INTO medical_records VALUES (1,  1, 1, '2025-01-10', 'Healthy, no concerns',              'None required',                     '2026-01-10');
INSERT INTO medical_records VALUES (2,  2, 2, '2025-01-15', 'Hypertension Stage 1',              'Prescribed Lisinopril, diet changes', '2025-04-15');
INSERT INTO medical_records VALUES (3,  5, 3, '2025-01-20', 'Routine vaccination',               'Administered DTaP booster',          '2025-07-20');
INSERT INTO medical_records VALUES (4,  4, 2, '2025-02-01', 'Elevated cholesterol, mild angina', 'Statin therapy, lifestyle changes',  '2025-05-01');
INSERT INTO medical_records VALUES (5,  3, 1, '2025-02-10', 'Upper respiratory infection',       'Antibiotics 10-day course',          '2025-02-24');
INSERT INTO medical_records VALUES (6,  6, 4, '2025-02-18', 'ACL sprain, grade 2',              'Physical therapy, brace fitted',      '2025-03-18');
INSERT INTO medical_records VALUES (7,  7, 5, '2025-03-05', 'Contact dermatitis',               'Topical corticosteroid',              '2025-04-05');
INSERT INTO medical_records VALUES (8,  9, 1, '2025-03-12', 'Chronic migraine',                 'Triptan prescription, avoid triggers', '2025-06-12');
INSERT INTO medical_records VALUES (9,  10, 6, '2025-03-20', 'Type 2 diabetes, controlled',     'Metformin, blood sugar monitoring',    '2025-06-20');
INSERT INTO medical_records VALUES (10, 8, 3, '2025-01-08', 'Ear infection',                    'Amoxicillin drops, follow-up 2 weeks', '2025-01-22');

-- ─── Billing ────────────────────────────────────────────────

INSERT INTO billing VALUES (1,  1, 1,  250.00, 200.00,  50.00, 'paid',    '2025-01-10');
INSERT INTO billing VALUES (2,  2, 2,  350.00, 280.00,  70.00, 'paid',    '2025-01-15');
INSERT INTO billing VALUES (3,  5, 3,  150.00, 150.00,   0.00, 'paid',    '2025-01-20');
INSERT INTO billing VALUES (4,  4, 4,  450.00, 315.00, 135.00, 'paid',    '2025-02-01');
INSERT INTO billing VALUES (5,  3, 5,  200.00, 160.00,  40.00, 'paid',    '2025-02-10');
INSERT INTO billing VALUES (6,  6, 6,  500.00, 400.00, 100.00, 'paid',    '2025-02-18');
INSERT INTO billing VALUES (7,  7, 7,  175.00, 122.50,  52.50, 'paid',    '2025-03-05');
INSERT INTO billing VALUES (8,  9, 8,  225.00, 157.50,  67.50, 'pending', '2025-03-12');
INSERT INTO billing VALUES (9,  10, 9, 300.00, 300.00,   0.00, 'pending', '2025-03-20');
INSERT INTO billing VALUES (10, 8, 10, 150.00, 120.00,  30.00, 'pending', '2025-01-08');

-- ─── Indexes ────────────────────────────────────────────────

CREATE INDEX idx_appointments_patient ON appointments (patient_id);
CREATE INDEX idx_appointments_doctor ON appointments (doctor_id);
CREATE INDEX idx_prescriptions_patient ON prescriptions (patient_id);
CREATE INDEX idx_billing_patient ON billing (patient_id);

-- ─── Views ──────────────────────────────────────────────────

CREATE VIEW upcoming_appointments AS
SELECT a.id, p.name, d.name, a.appointment_date, a.appointment_time, a.status
FROM appointments a
INNER JOIN patients p ON p.id = a.patient_id
INNER JOIN doctors d ON d.id = a.doctor_id
WHERE a.status = 'scheduled';

CREATE VIEW outstanding_bills AS
SELECT b.id, p.name, b.amount, b.insurance_covered, b.patient_owes, b.billing_date
FROM billing b
INNER JOIN patients p ON p.id = b.patient_id
WHERE b.status = 'pending';

-- ─── Triggers ───────────────────────────────────────────────

CREATE TRIGGER trg_record_after_appointment AFTER UPDATE ON appointments
BEGIN
    INSERT INTO medical_records (id, patient_id, doctor_id, visit_date, diagnosis, treatment, follow_up_date)
    VALUES (100 + NEW.id, NEW.patient_id, NEW.doctor_id, NEW.appointment_date, 'Pending review', 'Pending', '');
END;
