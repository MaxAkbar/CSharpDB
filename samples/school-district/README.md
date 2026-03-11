# Maplewood Unified School District

Maplewood Unified School District is a K-12 education sample with teachers, students, courses, enrollments, classrooms, schedules, and attendance. It works well for testing many-to-many relationships and calendar-style data.

## Files

- `schema.sql` - schema plus seed data
- `procedures.json` - `GetStudentCourses`, `RecordAttendance`

## What It Showcases

- Student/course enrollment joins and roster-style reporting
- Views for read-heavy admin workflows: `class_roster`, `teacher_schedule`
- Trigger-generated child rows through `trg_enrollment_attendance`
- Defaulted procedure parameters for semester and year
- Time and scheduling data without needing advanced SQL features

## Good Starting Points

- `SELECT * FROM class_roster ORDER BY year DESC, semester, grade;`
- `SELECT * FROM teacher_schedule ORDER BY day_of_week, start_time;`
- `EXEC GetStudentCourses studentId=1, semester='Fall', year=2024;`
- `EXEC RecordAttendance attendanceId=3001, studentId=1, courseId=1, attendanceDate='2025-02-01';`

## Load

```text
csdb> .read samples/school-district/schema.sql
```
