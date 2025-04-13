SELECT * FROM healthcare.healthcare_dataset;

-- Creating a second table to prevent making changes to the raw data incase of mistakes.
CREATE TABLE healthcare1
LIKE healthcare_dataset;

SELECT * FROM healthcare1;

-- Now we've created a second table, we will insert the raw data into that table
INSERT healthcare1
SELECT * FROM healthcare_dataset;

SELECT * FROM healthcare1;

--- DATA CLEANING  
-- To clean the data, I'll first check for duplicates
SELECT *, ROW_NUMBER() OVER(PARTITION BY Name, Age, Gender, Blood_Type, Medical_Condition, 
				  Doctor, Date_of_Admission, Hospital, Insurance_Provider, 
                  Billing_Amount, Room_Number, Admission_Type, Discharge_Date, Medication, 
                  Test_Results) AS row_num
FROM healthcare1;

-- If row_num > 1, we have duplicates. 
-- To check if row_num > 1 we create a cte or subquery.
-- Using a cte..
WITH duplicate_cte AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY Name, Age, Gender, Blood_Type, Medical_Condition, 
				      Doctor, Date_of_Admission, Hospital, Insurance_Provider, Billing_Amount, 
                      Room_Number, Admission_Type, Discharge_Date, Medication, Test_Results) AS row_num FROM healthcare1)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

-- Using a subquery..
SELECT * FROM 
             (SELECT *, ROW_NUMBER() OVER(PARTITION BY Name, Age, Gender, Blood_Type, Medical_Condition, 
			  Doctor, Date_of_Admission, Hospital, Insurance_Provider, Billing_Amount, 
              Room_Number, Admission_Type, Discharge_Date, Medication, Test_Results) AS row_num FROM healthcare1)
AS DUP
WHERE row_num > 1;
-- No duplicates were found

SELECT * FROM healthcare1;

-- The names are not in the same case which doesn't make the data look clean 
-- We can fix that using the upper() and lower() with substring() functions

SELECT Name, CONCAT(UPPER(SUBSTRING(Name,1,1)), LOWER(SUBSTRING(Name, 2,locate(' ',Name)-1)),
    UPPER(SUBSTRING(Name,Locate(' ', Name)+1,1)),
    LOWER(SUBSTRING(Name,Locate(' ', Name)+2))) 
FROM healthcare1;

-- Inserting the cleaned names into our table
UPDATE healthcare1
SET Name = CONCAT(UPPER(SUBSTRING(Name,1,1)), LOWER(SUBSTRING(Name, 2,locate(' ',Name)-1)),
    UPPER(SUBSTRING(Name,Locate(' ', Name)+1,1)),
    LOWER(SUBSTRING(Name,Locate(' ', Name)+2)));

SELECT * FROM healthcare1;

-- Our dates (date of admssion and discharge date) are text columns so I'll be changing them to date columns

ALTER TABLE healthcare1
MODIFY COLUMN Date_of_Admission DATE;

ALTER TABLE healthcare1
MODIFY COLUMN Discharge_Date date;


--- EXPLORATORY DATA ANALYSIS (EDA)
-- To find total patients
SELECT count(Name) AS Total_patients
FROM healthcare1;

-- Average age of patients
SELECT AVG(Age) as avg_age
FROM healthcare1;

SELECT ROUND(AVG(Age), 1) AS AVGG_AGE
FROM healthcare1;

-- To find the most dispensed medication
SELECT Medication, COUNT(Name) AS Total_patients
FROM healthcare1
GROUP BY Medication
ORDER BY 2 DESC;

-- To find out the most used insurance provider
SELECT Insurance_Provider, COUNT(Name) AS Total_patients
FROM healthcare1
GROUP BY Insurance_Provider
ORDER BY 2 DESC;

-- The most common admission type
SELECT Admission_Type, COUNT(Name) AS Total_patients
FROM healthcare1
GROUP BY Admission_Type
ORDER BY 2 DESC;

-- Which gender had the highest hospital admissions?
SELECT Gender, COUNT(Name) AS Total_patients
FROM Healthcare1
GROUP BY Gender
ORDER BY Total_patients DESC;

-- Which blood type had the highest hospital admissions?
SELECT Blood_Type, COUNT(Name) AS Total_patients
FROM Healthcare1
GROUP BY Blood_Type
ORDER BY Total_patients DESC;

-- Which medical condition had the highest hospital admissions?
SELECT Medical_Condition, COUNT(Name) AS Total_patients
FROM Healthcare1
GROUP BY Medical_Condition
ORDER BY 2 DESC;


-- What are the top three medical conditions reported for urgent vs. emergency hospital admissions?
WITH Condition_Admission AS 
                 (SELECT Admission_Type, Medical_Condition, COUNT(Medical_Condition) AS Total_Medical_Condition FROM healthcare1
                  WHERE Admission_Type IN ('Urgent', 'Emergency')
                  GROUP BY Admission_Type, Medical_Condition),
	 Condition_Admission_Rank AS 
                 (SELECT *,  DENSE_RANK() OVER (PARTITION BY Admission_Type ORDER BY Total_Medical_Condition DESC) AS ranking
                  FROM Condition_Admission)
SELECT Admission_Type, Medical_Condition, Total_Medical_Condition
FROM Condition_Admission_Rank
WHERE ranking <= 3;


-- Most common medical conditions with hospital admissions over the years
WITH Condition_Year AS 
          (SELECT Medical_Condition, YEAR(Date_of_Admission) AS years, COUNT(Medical_Condition) AS Total_Medical_Condition
           FROM healthcare1
           GROUP BY YEAR(Date_of_Admission), Medical_Condition), 
  Condition_Year_Rank AS 
		  (SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY Total_Medical_Condition DESC) AS ranking
           FROM Condition_Year)
SELECT Medical_Condition, years, Total_Medical_Condition 
FROM Condition_Year_Rank
WHERE ranking  <= 1;


-- What are the most common medical conditions of each blood type?
WITH Condition_Year AS 
          (SELECT Blood_Type, Medical_Condition, COUNT(Medical_Condition) Total_Medical_Condition
           FROM healthcare1
           GROUP BY Blood_Type, Medical_Condition), 
  Condition_Year_Rank AS 
		  (SELECT *, DENSE_RANK() OVER (PARTITION BY Blood_Type ORDER BY Total_Medical_Condition DESC) AS ranking
           FROM Condition_Year)
SELECT  Blood_Type, Medical_Condition, Total_Medical_Condition
FROM Condition_Year_Rank
WHERE ranking <= 1;

-- What are the most common medical conditions of each gender
WITH Medication_gender AS 
             (SELECT Gender, Medical_Condition, COUNT(Medical_Condition) AS Total_Medical_Condition
              FROM healthcare1
              GROUP BY Gender, Medical_Condition), 
Medication_gender_condition AS 
			(SELECT *, DENSE_RANK() OVER (PARTITION BY Gender ORDER BY Total_Medical_Condition DESC) AS Ranking
             FROM Medication_gender)
SELECT Gender, Medical_Condition, Total_Medical_Condition
FROM Medication_gender_condition
WHERE Ranking <= 1;

-- Which medical condition had the highest length of stay and the average length of stay per admission of each medical condition?
WITH DD AS (SELECT Medical_Condition, Date_of_Admission, Discharge_Date,
           DATEDIFF(Discharge_Date, Date_of_Admission) AS date_difference
    FROM healthcare1)
SELECT Medical_Condition, SUM(date_difference) AS Total_Stay, COUNT(*) AS Total_Admissions, 
       ROUND(AVG(date_difference), 1) AS Avg_Stay_Per_Admission
FROM DD
GROUP BY Medical_Condition
ORDER BY Total_Stay DESC;

-- Which age had the highest length of stay and the average length of stay per admission of each age?
WITH DD AS (SELECT Age, Date_of_Admission, Discharge_Date,
           DATEDIFF(Discharge_Date, Date_of_Admission) AS date_difference
    FROM healthcare1)
SELECT Age, SUM(date_difference) AS Total_Stay, COUNT(*) AS Total_Admissions, 
       ROUND(AVG(date_difference), 1) AS Avg_Stay_Per_Admission
FROM DD
GROUP BY Age
ORDER BY Total_Stay DESC;


-- What is the most common medication by test results?
WITH Medication_test AS 
             (SELECT Medication, Test_Results, COUNT(Medication) AS Total_Medication
              FROM healthcare1
              GROUP BY Medication, Test_Results), 
Ranked_Medication AS 
			(SELECT *, DENSE_RANK() OVER (PARTITION BY Test_Results ORDER BY Total_Medication DESC) AS Ranking
             FROM Medication_test)
SELECT Medication, Test_Results, Total_Medication 
FROM Ranked_Medication
WHERE Ranking <= 1;


-- Top medication administered by medical condition
WITH Medication_test AS 
             (SELECT Medication, Medical_Condition, COUNT(Medication) AS Total_Medication
              FROM healthcare1
              GROUP BY Medication, Medical_Condition), 
Ranked_Medication AS 
			(SELECT *, DENSE_RANK() OVER (PARTITION BY Medical_Condition ORDER BY Total_Medication DESC) AS Ranking
             FROM Medication_test)
SELECT Medication, Medical_Condition, Total_Medication
FROM Ranked_Medication
WHERE Ranking <= 1;








