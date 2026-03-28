UPDATE Professor
SET Salary = Salary + 1500
WHERE IsTenured = false AND Salary < 55000;

UPDATE Student
SET IsActive = false
WHERE EnrollmentYear = 2019 AND IsActive = true;

UPDATE Enrollment
SET Grade = 100
WHERE Grade > 91 AND Grade < 100 AND CourseID > 50;

DELETE FROM Enrollment
WHERE Grade < 78 AND StudentID = 16;

DELETE FROM Professor
WHERE Salary < 50000 AND IsTenured = false;