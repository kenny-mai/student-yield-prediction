SELECT 
	NULL AS individual__c
	, id AS round_app_id
	, round_app_name AS round_app_name
	, app_date AS application_date
	, CONCAT('SA ', school_name_as_per_app_target_file_best_offer) AS accepted_school
	, grade_applying_for AS grade
	, students_home__latitude__s
	, students_home__longitude__s
	, NULL AS address
	, NULL AS city__c
	, NULL AS zip_code__c
	, NULL AS gender__c
	, NULL AS how_did_you_hear_about_us__c
	, parent_contact__c AS parent_id
	, NULL AS parent_uuid
	, priority_group_verbiage__c AS preference_structure_verbiage
	, best_offer_rank AS program_rank
	, CASE
		WHEN grade SIMILAR TO ('4|5|6') THEN 1
		ELSE 0
	END AS summer_intensive
	, datediff(
			DAY
		, '2024-04-04'::date
		-- needs updated
		, '{date}'::DATE
	) AS days_since_offer
	, CASE
			WHEN best_offer_rank = 1 THEN 1
		ELSE 0
	END AS accepted_first_rank
	, CASE
			WHEN preference_structure_verbiage SIMILAR TO ('Sibling Attending Same School|Sibling Attending Co-Located School|Child of Staff') 
		THEN 1
		ELSE 0
	END AS had_enrolled_sib
FROM
	prod_scholar_acquisition_yield.prod_scholar_acquisition_yield_signal
WHERE
	has_current_offer = TRUE