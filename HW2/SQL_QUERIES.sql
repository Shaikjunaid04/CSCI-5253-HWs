--QUESTION 1

--How many animals of each type have outcomes?
--I.e. how many cats, dogs, birds etc. Note that this question is asking about number of animals, 
--not number of outcomes, so animals with multiple outcomes should be counted only once.


SELECT animal_type, count(*) FROM animal GROUP BY animal_type;


--QUESTION 2

--How many animals are there with more than 1 outcome?

SELECT
    COUNT(animal_id) AS "Animals with More than 1 Outcome"
FROM (
    SELECT
        animal_id
    FROM
        outcome_event
    GROUP BY
        animal_id
    HAVING
        COUNT(*) > 1
) AS Subquery;

--QUESTION 3

--What are the top 5 months for outcomes? 
--Calendar months in general, not months of a particular year. This means answer will be like April, October, etc rather than April 2013, October 2018, 



SELECT
    TO_CHAR(outcome_event.datetime, 'Month') AS "Month",
    COUNT(*) AS "Total Outcomes"
FROM
    outcome_event
GROUP BY
    TO_CHAR(outcome_event.datetime, 'Month')
ORDER BY
    "Total Outcomes" DESC
LIMIT 5;

--QUESTION 4

-- A "Kitten" is a "Cat" who is less than 1 year old. A "Senior cat" is a "Cat" who is over 10 years old. An "Adult" is a cat who is between 1 and 10 years old.
-- What is the total number of kittens, adults, and seniors, whose outcome is "Adopted"?
-- Conversely, among all the cats who were "Adopted", what is the total number of kittens, adults, and seniors?

-- Here we try to count only the cats who were "Adopted" within each of the three age group categories (kittens, adults, and seniors) 
   
SELECT
    CASE
        WHEN age_in_years < 1 THEN 'Kitten'
        WHEN age_in_years >= 1 AND age_in_years <= 10 THEN 'Adult'
        WHEN age_in_years > 10 THEN 'Senior'
        ELSE 'Other'
    END AS "Age Category",
    COUNT(*) AS "Total Cats Adopted"
FROM (
    SELECT
        a.animal_id,
        a.animal_type,
        DATE_PART('year', age(oe.datetime, a.date_of_birth)) AS age_in_years
    FROM
        animal AS a
        JOIN outcome_event AS oe ON a.animal_id = oe.animal_id
        JOIN outcome_type AS ot ON oe.outcome_type_id = ot.outcome_type_id
    WHERE
        ot.outcome_type = 'Adoption'
        AND a.animal_type = 'Cat'
) AS AdoptedCats
GROUP BY
    "Age Category";


--QUESTION 5

--For each date, what is the cumulative total of outcomes up to and including this date?
--Note: this type of question is usually used to create dashboard for progression of quarterly metrics. 
--In SQL, this is usually accomplished using something called Window Functions. You'll need to research and learn this on your own!

   
SELECT
    "date",
    COUNT(*) AS "Daily Outcomes",
    SUM(COUNT(*)) OVER (ORDER BY "date") AS "Cumulative Total Outcomes"
FROM (
    SELECT
        date(datetime) AS "date"
    FROM
        outcome_event
) AS DateEvents
GROUP BY
    "date"
ORDER BY
    "date";