-- ============================================================
-- Maplewood Unified School District
-- ============================================================
-- A K-12 school district with teachers, students, courses,
-- enrollments, classrooms, scheduling, and attendance.
-- ============================================================

-- ─── Tables ─────────────────────────────────────────────────

CREATE TABLE teachers (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    department  TEXT NOT NULL,
    hire_year   INTEGER NOT NULL,
    is_active   INTEGER NOT NULL
);

CREATE TABLE students (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    grade_level     INTEGER NOT NULL,
    enrollment_year INTEGER NOT NULL,
    guardian_name   TEXT NOT NULL,
    guardian_phone  TEXT NOT NULL
);

CREATE TABLE courses (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    teacher_id      INTEGER NOT NULL,
    department      TEXT NOT NULL,
    credits         INTEGER NOT NULL,
    max_enrollment  INTEGER NOT NULL
);

CREATE TABLE enrollments (
    id          INTEGER PRIMARY KEY,
    student_id  INTEGER NOT NULL,
    course_id   INTEGER NOT NULL,
    semester    TEXT NOT NULL,
    year        INTEGER NOT NULL,
    grade       TEXT
);

CREATE TABLE classrooms (
    id              INTEGER PRIMARY KEY,
    building        TEXT NOT NULL,
    room_number     TEXT NOT NULL,
    capacity        INTEGER NOT NULL,
    has_projector   INTEGER NOT NULL
);

CREATE TABLE course_schedule (
    id              INTEGER PRIMARY KEY,
    course_id       INTEGER NOT NULL,
    classroom_id    INTEGER NOT NULL,
    day_of_week     TEXT NOT NULL,
    start_time      TEXT NOT NULL,
    end_time        TEXT NOT NULL
);

CREATE TABLE attendance (
    id              INTEGER PRIMARY KEY,
    student_id      INTEGER NOT NULL,
    course_id       INTEGER NOT NULL,
    attendance_date TEXT NOT NULL,
    status          TEXT NOT NULL
);

-- ─── Teachers ───────────────────────────────────────────────

INSERT INTO teachers VALUES (1, 'Margaret Holloway',  'mholloway@maplewood.edu',  'Mathematics',      2010, 1);
INSERT INTO teachers VALUES (2, 'Daniel Reeves',      'dreeves@maplewood.edu',    'English',          2015, 1);
INSERT INTO teachers VALUES (3, 'Yuki Tanaka',        'ytanaka@maplewood.edu',    'Science',          2012, 1);
INSERT INTO teachers VALUES (4, 'Carlos Mendez',      'cmendez@maplewood.edu',    'History',          2018, 1);
INSERT INTO teachers VALUES (5, 'Rachel Foster',      'rfoster@maplewood.edu',    'Science',          2020, 1);
INSERT INTO teachers VALUES (6, 'James O''Brien',     'jobrien@maplewood.edu',    'Physical Education', 2008, 1);
INSERT INTO teachers VALUES (7, 'Aisha Bakari',       'abakari@maplewood.edu',    'Art',              2019, 1);
INSERT INTO teachers VALUES (8, 'Steven Park',        'spark@maplewood.edu',      'Mathematics',      2022, 1);

-- ─── Students ───────────────────────────────────────────────

INSERT INTO students VALUES (1,  'Emma Thompson',     9,  2024, 'Karen Thompson',    '555-3001');
INSERT INTO students VALUES (2,  'Liam Rodriguez',    9,  2024, 'Rosa Rodriguez',    '555-3002');
INSERT INTO students VALUES (3,  'Sophia Patel',      10, 2023, 'Raj Patel',         '555-3003');
INSERT INTO students VALUES (4,  'Noah Williams',     10, 2023, 'Janet Williams',    '555-3004');
INSERT INTO students VALUES (5,  'Ava Chen',          11, 2022, 'Wei Chen',          '555-3005');
INSERT INTO students VALUES (6,  'Mason Johnson',     11, 2022, 'Derek Johnson',     '555-3006');
INSERT INTO students VALUES (7,  'Isabella Brown',    12, 2021, 'Patricia Brown',    '555-3007');
INSERT INTO students VALUES (8,  'Ethan Davis',       12, 2021, 'Mark Davis',        '555-3008');
INSERT INTO students VALUES (9,  'Mia Wilson',        9,  2024, 'Sharon Wilson',     '555-3009');
INSERT INTO students VALUES (10, 'Lucas Martinez',    10, 2023, 'Ana Martinez',      '555-3010');
INSERT INTO students VALUES (11, 'Charlotte Lee',     11, 2022, 'David Lee',         '555-3011');
INSERT INTO students VALUES (12, 'Oliver Garcia',     12, 2021, 'Maria Garcia',      '555-3012');
INSERT INTO students VALUES (13, 'Amelia Taylor',     9,  2024, 'Brian Taylor',      '555-3013');
INSERT INTO students VALUES (14, 'Jack Anderson',     10, 2023, 'Susan Anderson',    '555-3014');
INSERT INTO students VALUES (15, 'Harper Moore',      11, 2022, 'Michael Moore',     '555-3015');

-- ─── Courses ────────────────────────────────────────────────

INSERT INTO courses VALUES (1,  'Algebra I',             1, 'Mathematics', 4, 30);
INSERT INTO courses VALUES (2,  'Geometry',              8, 'Mathematics', 4, 30);
INSERT INTO courses VALUES (3,  'AP Calculus',           1, 'Mathematics', 5, 25);
INSERT INTO courses VALUES (4,  'English Literature',    2, 'English',     4, 30);
INSERT INTO courses VALUES (5,  'Creative Writing',      2, 'English',     3, 25);
INSERT INTO courses VALUES (6,  'Biology',               3, 'Science',     4, 28);
INSERT INTO courses VALUES (7,  'Chemistry',             5, 'Science',     4, 28);
INSERT INTO courses VALUES (8,  'World History',         4, 'History',     4, 30);
INSERT INTO courses VALUES (9,  'US Government',         4, 'History',     3, 30);
INSERT INTO courses VALUES (10, 'Studio Art',            7, 'Art',         3, 20);

-- ─── Enrollments ────────────────────────────────────────────

INSERT INTO enrollments VALUES (1,  1,  1, 'Fall',   2024, 'B+');
INSERT INTO enrollments VALUES (2,  1,  4, 'Fall',   2024, 'A-');
INSERT INTO enrollments VALUES (3,  1,  6, 'Fall',   2024, 'A');
INSERT INTO enrollments VALUES (4,  2,  1, 'Fall',   2024, 'C+');
INSERT INTO enrollments VALUES (5,  2,  8, 'Fall',   2024, 'B');
INSERT INTO enrollments VALUES (6,  3,  2, 'Fall',   2024, 'A');
INSERT INTO enrollments VALUES (7,  3,  7, 'Fall',   2024, 'A-');
INSERT INTO enrollments VALUES (8,  4,  2, 'Fall',   2024, 'B-');
INSERT INTO enrollments VALUES (9,  4,  4, 'Fall',   2024, 'B+');
INSERT INTO enrollments VALUES (10, 5,  3, 'Fall',   2024, 'A');
INSERT INTO enrollments VALUES (11, 5,  5, 'Fall',   2024, 'A+');
INSERT INTO enrollments VALUES (12, 6,  7, 'Fall',   2024, 'B');
INSERT INTO enrollments VALUES (13, 6,  8, 'Fall',   2024, 'B-');
INSERT INTO enrollments VALUES (14, 7,  3, 'Fall',   2024, 'A-');
INSERT INTO enrollments VALUES (15, 7,  9, 'Fall',   2024, 'A');
INSERT INTO enrollments VALUES (16, 8,  9, 'Fall',   2024, 'B+');
INSERT INTO enrollments VALUES (17, 8,  10, 'Fall',  2024, 'A');
INSERT INTO enrollments VALUES (18, 9,  1, 'Spring', 2025, 'B');
INSERT INTO enrollments VALUES (19, 9,  6, 'Spring', 2025, 'B+');
INSERT INTO enrollments VALUES (20, 10, 4, 'Spring', 2025, 'A-');
INSERT INTO enrollments VALUES (21, 11, 3, 'Spring', 2025, 'B+');
INSERT INTO enrollments VALUES (22, 12, 5, 'Spring', 2025, 'A');
INSERT INTO enrollments VALUES (23, 13, 1, 'Spring', 2025, 'A-');
INSERT INTO enrollments VALUES (24, 14, 7, 'Spring', 2025, 'B');
INSERT INTO enrollments VALUES (25, 15, 9, 'Spring', 2025, 'A');

-- ─── Classrooms ─────────────────────────────────────────────

INSERT INTO classrooms VALUES (1, 'Main Building',    '101', 30, 1);
INSERT INTO classrooms VALUES (2, 'Main Building',    '102', 30, 1);
INSERT INTO classrooms VALUES (3, 'Main Building',    '201', 25, 1);
INSERT INTO classrooms VALUES (4, 'Science Wing',     'S101', 28, 1);
INSERT INTO classrooms VALUES (5, 'Science Wing',     'S102', 28, 1);
INSERT INTO classrooms VALUES (6, 'Arts Building',    'A101', 20, 0);
INSERT INTO classrooms VALUES (7, 'Gymnasium',        'GYM1', 50, 0);
INSERT INTO classrooms VALUES (8, 'Main Building',    '203', 30, 1);

-- ─── Course Schedule ────────────────────────────────────────

INSERT INTO course_schedule VALUES (1,  1, 1, 'Monday',    '08:00', '09:15');
INSERT INTO course_schedule VALUES (2,  1, 1, 'Wednesday', '08:00', '09:15');
INSERT INTO course_schedule VALUES (3,  2, 2, 'Monday',    '09:30', '10:45');
INSERT INTO course_schedule VALUES (4,  2, 2, 'Wednesday', '09:30', '10:45');
INSERT INTO course_schedule VALUES (5,  3, 3, 'Tuesday',   '08:00', '09:15');
INSERT INTO course_schedule VALUES (6,  3, 3, 'Thursday',  '08:00', '09:15');
INSERT INTO course_schedule VALUES (7,  4, 8, 'Monday',    '11:00', '12:15');
INSERT INTO course_schedule VALUES (8,  4, 8, 'Wednesday', '11:00', '12:15');
INSERT INTO course_schedule VALUES (9,  5, 8, 'Tuesday',   '11:00', '12:15');
INSERT INTO course_schedule VALUES (10, 6, 4, 'Tuesday',   '09:30', '10:45');
INSERT INTO course_schedule VALUES (11, 6, 4, 'Thursday',  '09:30', '10:45');
INSERT INTO course_schedule VALUES (12, 7, 5, 'Monday',    '13:00', '14:15');
INSERT INTO course_schedule VALUES (13, 7, 5, 'Wednesday', '13:00', '14:15');
INSERT INTO course_schedule VALUES (14, 8, 2, 'Tuesday',   '13:00', '14:15');
INSERT INTO course_schedule VALUES (15, 9, 1, 'Thursday',  '11:00', '12:15');
INSERT INTO course_schedule VALUES (16, 10, 6, 'Friday',   '09:00', '11:00');

-- ─── Attendance ─────────────────────────────────────────────

INSERT INTO attendance VALUES (1,  1, 1, '2025-01-06', 'present');
INSERT INTO attendance VALUES (2,  1, 1, '2025-01-08', 'present');
INSERT INTO attendance VALUES (3,  2, 1, '2025-01-06', 'present');
INSERT INTO attendance VALUES (4,  2, 1, '2025-01-08', 'absent');
INSERT INTO attendance VALUES (5,  1, 4, '2025-01-06', 'present');
INSERT INTO attendance VALUES (6,  1, 4, '2025-01-08', 'present');
INSERT INTO attendance VALUES (7,  3, 2, '2025-01-06', 'present');
INSERT INTO attendance VALUES (8,  3, 2, '2025-01-08', 'present');
INSERT INTO attendance VALUES (9,  4, 2, '2025-01-06', 'tardy');
INSERT INTO attendance VALUES (10, 4, 2, '2025-01-08', 'present');
INSERT INTO attendance VALUES (11, 5, 3, '2025-01-07', 'present');
INSERT INTO attendance VALUES (12, 5, 3, '2025-01-09', 'present');
INSERT INTO attendance VALUES (13, 7, 3, '2025-01-07', 'present');
INSERT INTO attendance VALUES (14, 7, 3, '2025-01-09', 'absent');
INSERT INTO attendance VALUES (15, 6, 7, '2025-01-06', 'present');
INSERT INTO attendance VALUES (16, 6, 7, '2025-01-08', 'present');
INSERT INTO attendance VALUES (17, 8, 9, '2025-01-09', 'present');
INSERT INTO attendance VALUES (18, 8, 10, '2025-01-10', 'present');
INSERT INTO attendance VALUES (19, 9, 1, '2025-01-13', 'present');
INSERT INTO attendance VALUES (20, 13, 1, '2025-01-13', 'tardy');

-- ─── Indexes ────────────────────────────────────────────────

CREATE INDEX idx_enrollments_student ON enrollments (student_id);
CREATE INDEX idx_enrollments_course ON enrollments (course_id);
CREATE INDEX idx_attendance_student ON attendance (student_id);
CREATE INDEX idx_schedule_course ON course_schedule (course_id);

-- ─── Views ──────────────────────────────────────────────────

CREATE VIEW class_roster AS
SELECT e.id, s.name, c.name, e.semester, e.year, e.grade
FROM enrollments e
INNER JOIN students s ON s.id = e.student_id
INNER JOIN courses c ON c.id = e.course_id;

CREATE VIEW teacher_schedule AS
SELECT cs.id, t.name, c.name, cr.building, cr.room_number, cs.day_of_week, cs.start_time, cs.end_time
FROM course_schedule cs
INNER JOIN courses c ON c.id = cs.course_id
INNER JOIN teachers t ON t.id = c.teacher_id
INNER JOIN classrooms cr ON cr.id = cs.classroom_id;

-- ─── Triggers ───────────────────────────────────────────────

CREATE TRIGGER trg_enrollment_attendance AFTER INSERT ON enrollments
BEGIN
    INSERT INTO attendance (id, student_id, course_id, attendance_date, status)
    VALUES (1000 + NEW.id, NEW.student_id, NEW.course_id, '2025-01-01', 'enrolled');
END;
