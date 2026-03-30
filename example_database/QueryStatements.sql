SELECT Student.StudentName AS SName, Course.CourseTitle AS CName, Enrollment.Points AS Points
FROM Student JOIN Enrollment ON Student.StudentID = Enrollment.StudentID JOIN Course ON Enrollment.CourseID = Course.CourseID
WHERE Student.IsActive = true AND Enrollment.Grade >= 85 AND Course.Credits = 4;

SELECT Professor.ProfName AS PName, Department.DeptName AS DName
FROM Professor JOIN Department ON Professor.DeptID = Department.DeptID
WHERE Professor.IsTenured = false AND Professor.Salary < 60000;

SELECT StudentID AS ID, EnrollmentYear AS Year
FROM Student
WHERE EnrollmentYear > 2019 AND MajorDeptID != 5 AND IsActive = false;

SELECT ProfName AS Name, Salary * 12 AS AnnualSalary
FROM Professor
WHERE Salary >= 50000 AND DeptID <= 10;

SELECT StudentName AS PersonName FROM Student WHERE IsActive = true
UNION ALL
SELECT ProfName AS PersonName FROM Professor WHERE IsTenured = true;
