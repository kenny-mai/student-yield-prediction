WITH
student AS (
	SELECT 
		id
		, CASE
			WHEN students_home__latitude__s IS NULL THEN 40.776676
			ELSE students_home__latitude__s
		END
		, CASE
			WHEN students_home__longitude__s IS NULL THEN -73.971321
			ELSE students_home__longitude__s
		END
		, best_ever_cy_assignment__c
		, best_ever_cy_assignment_rank__c AS accepted_first_rank
		, free_uniform_order_date__c AS uniform_ordered_date
		, is_english_language_learner__c AS ell_status
		, is_homeless__c AS homeless_status
		, survey_representative_first_name__c AS recruiter_first_name
		, survey_representative_last_name__c AS recruiter_last_name
	FROM
		sacs_salesforce.school_application_mirror.prod_student__c psc
	WHERE
		best_ever_cy_assignment_status__c = 'Accepted'
		AND archived__c = FALSE
)
, student_contact AS (
	SELECT
		DISTINCT
		org_specific_id__c AS esd_id
		, student__c AS student
	FROM
		sacs_salesforce.school_application_mirror.prod_contact
)
, parent_contact AS (
	SELECT
		DISTINCT
		id AS parent_id
		, lastname
		, utm_source__c AS utm_source
		, utm_medium__c AS utm_medium
		, utm_campaign__c AS utm_campaign
		, utm_content__c AS utm_content
		, uuid__c AS parent_uuid
	FROM
		sacs_salesforce.school_application_mirror.prod_contact
)
, lottery AS (
	SELECT 
		id
		, round_application__c
		, account_school__c
		, offer_date__c
		, min(offer_date__c) OVER(
			PARTITION BY round_application__c
		) AS first_offer_date
	FROM
		sacs_salesforce.school_application_mirror.prod_waitlist_school_ranking__c
)
, application AS (
	SELECT 
		id
		, name
		, student__c
		, parent_contact__c
		, current_grade__c AS grade
	FROM
		sacs_salesforce.school_application_mirror.prod_application__c
	WHERE
		status__c = 'Submitted'
		AND (
			ineligibility_reason__c IS NULL
				OR ineligibility_reason__c NOT IN (
					'Age - Too Old', 'Test Application', 'Ineligible', 'Duplicate', 'Age - Too Young', 'No Proof', 'Test Application'
				)
		)
			AND recordtypeid != '0125f000000AoNiAAK'
			AND archived__c = FALSE
)
, account AS (
	SELECT
		id
		, name AS accepted_school
	FROM
		sacs_salesforce.school_application_mirror.prod_account
)
, hed AS (
	SELECT
		DISTINCT
        app_round_application__c
		, program_rank__c
		, priority_group_verbiage__c AS had_enrolled_sib
	FROM
		sacs_salesforce.school_application_mirror.prod_hed__application__c
)
, referral AS (
	SELECT
		application_id
		, verified_parent
	FROM
		raw_data_science.raw_sy_23_24_parent_referal_apps
)
, present_days AS (
	SELECT
		sa_scholar_id
		, count(*) AS days_active
	FROM
		sacs.fact_daily_scholar_status fdss
	WHERE
		fdss.date_key >= '2023-08-14'
		AND (
			attendance_status IN (
				'T', 'P'
			)
				OR excused = TRUE
		)
		AND in_session = TRUE
	GROUP BY
		1
)
, attended_five AS (
	SELECT
		DISTINCT
		dsoi.sa_scholar_id
	FROM
		present_days
	INNER JOIN sacs.dim_scholar_other_info dsoi
	ON
		present_days.sa_scholar_id = dsoi.sa_scholar_id
	WHERE
		ispreregistered = 'No'
		AND days_active >= 5
		AND (
			exit_reason_id IS NULL
				OR exit_reason_id != 78
		)
)
, splash AS (
	SELECT
		SUBSTRING(
			prod_splash_tours.salesforce_id::VARCHAR
			, "POSITION"(
				prod_splash_tours.salesforce_id::VARCHAR
				, ':'::VARCHAR
			) + 1
			, 50
		) AS splash_parent_id
		,
                    CASE
			WHEN lower(prod_splash_tours.event_name::VARCHAR) ILIKE '%uniform%'::VARCHAR THEN 'Uniform Fitting'::VARCHAR
			WHEN lower(prod_splash_tours.event_name::VARCHAR) ILIKE '%orientation%'::VARCHAR THEN 'Orientation'::VARCHAR
			WHEN lower(prod_splash_tours.event_type::VARCHAR) ILIKE '%webinars'::VARCHAR THEN 'Virtual Event'::VARCHAR
			WHEN lower(prod_splash_tours.event_name::VARCHAR) ILIKE 'welcome to success academy with eva moskowitz'::VARCHAR THEN 'Virtual Event'::VARCHAR
			WHEN lower(prod_splash_tours.event_name::VARCHAR) ILIKE 'summer intensive'::VARCHAR THEN 'Summer Intensive'::VARCHAR
			ELSE 'In-Person Event'::VARCHAR
		END AS event_type_bucket
		, MAX(
                        CASE
                            WHEN prod_splash_tours.status::VARCHAR = 'rsvp_yes'::VARCHAR OR prod_splash_tours.status::VARCHAR = 'checkin_yes'::VARCHAR OR prod_splash_tours.status::VARCHAR = 'checkin_no'::VARCHAR 
                            THEN 1
                                ELSE 0
                        END) AS rsvp
		, MAX(CASE
                        WHEN prod_splash_tours.status::VARCHAR = 'checkin_yes'::VARCHAR AND event_name = 'Success Academy Orientation' AND event_type = 'Prospective Family In-Person Events'
                        THEN 1
                            ELSE 0
                        END) AS orientation_checkin
		, MIN (
			CASE
				WHEN prod_splash_tours.status::VARCHAR = 'checkin_yes'::VARCHAR
					AND event_name = 'Success Academy Orientation'
					AND event_type = 'Prospective Family In-Person Events'
    						 THEN prod_splash_tours.checked_in
					ELSE NULL
				END
		) AS orientation_check_in_time
		, MAX(
                        CASE
                            WHEN prod_splash_tours.status::VARCHAR = 'rsvp_yes'::VARCHAR AND event_name = 'Success Academy Orientation' AND event_type = 'Prospective Family In-Person Events'
    						 THEN 1
                        		 ELSE 0
                        END) AS orientation_rsvp
		, MIN (
			CASE
				WHEN prod_splash_tours.status::VARCHAR = 'rsvp_yes'::VARCHAR
					AND event_name = 'Success Academy Orientation'
					AND event_type = 'Prospective Family In-Person Events'
    						 THEN substring(
						date_rsvped
						, 1
						, 10
					)
					ELSE NULL
				END
		) AS orientation_rsvp_date
		, MAX(CASE 
                    	WHEN prod_splash_tours.status::VARCHAR = 'checkin_yes'::VARCHAR AND prod_splash_tours.event_name::VARCHAR LIKE 'Community Support Day%'::VARCHAR
                    	THEN 1
                    	ELSE 0
                    END) AS community_support_day
		, MAX(CASE
                    	WHEN prod_splash_tours.status::VARCHAR = 'checkin_yes'::VARCHAR AND prod_splash_tours.event_name::VARCHAR NOT LIKE '%Orientation%' AND (prod_splash_tours.event_name::VARCHAR LIKE '%New Family%'::VARCHAR OR prod_splash_tours.event_name::VARCHAR LIKE 'Success Academy%'::VARCHAR)
                    	THEN 1
                    	ELSE 0
                    END
                    ) AS new_family_day
		, MAX(
                        CASE
                            WHEN prod_splash_tours.status::VARCHAR = 'checkin_yes'::VARCHAR OR prod_splash_tours.status::VARCHAR = 'checkin_no'::VARCHAR THEN 1
                            ELSE 0
                        END) AS attended
	FROM
		prod_data_science.prod_splash_tours AS prod_splash_tours
	GROUP BY
		SUBSTRING(
			prod_splash_tours.salesforce_id::VARCHAR
			, "POSITION"(
				prod_splash_tours.salesforce_id::VARCHAR
				, ':'::VARCHAR
			) + 1
			, 50
		)
		, 2
		,
                    CASE
			WHEN lower(prod_splash_tours.event_name::VARCHAR) ILIKE '%uniform%'::VARCHAR THEN 'Uniform Fitting'::VARCHAR
			WHEN lower(prod_splash_tours.event_name::VARCHAR) ILIKE '%orientation%'::VARCHAR THEN 'Orientation'::VARCHAR
			WHEN lower(prod_splash_tours.event_type::VARCHAR) ILIKE '%webinars'::VARCHAR THEN 'Virtual Event'::VARCHAR
			WHEN lower(prod_splash_tours.event_name::VARCHAR) ILIKE 'welcome to success academy with eva moskowitz'::VARCHAR THEN 'Virtual Event'::VARCHAR
			WHEN lower(prod_splash_tours.event_name::VARCHAR) ILIKE 'summer intensive'::VARCHAR THEN 'Summer Intensive'::VARCHAR
			ELSE 'In-Person Event'::VARCHAR
		END
)
,
yield_probability_tours_status AS
(
	SELECT
		application.name AS application_id
		, splash_parent_id
		, MAX(COALESCE(orientation_rsvp, 0)) AS orientation_rsvp
		, MIN(orientation_rsvp_date) AS orientation_rsvp_date
		, MAX(COALESCE(orientation_checkin, 0)) AS orientation_checkin
		, MIN(orientation_check_in_time) AS orientation_check_in_time
		, MAX(COALESCE(community_support_day, 0)) AS community_support_day
		, MAX(COALESCE(new_family_day, 0)) AS new_family_day
		, MAX(COALESCE(
                CASE
                    WHEN tours_agg.event_type_bucket = 'Virtual Event'::VARCHAR THEN tours_agg.attended
                    ELSE 0
                END, 0)) AS virtual_event_attended
		, MAX(COALESCE(
                CASE
                    WHEN tours_agg.event_type_bucket = 'In-Person Event'::VARCHAR THEN tours_agg.rsvp
                    ELSE 0
                END, 0)) AS in_person_event_rsvp
		, MAX(COALESCE(
                CASE
                    WHEN tours_agg.event_type_bucket = 'In-Person Event'::VARCHAR THEN tours_agg.attended
                    ELSE 0
                END, 0)) AS in_person_event_attended
	FROM
		application
	LEFT JOIN splash AS tours_agg 
       		ON
		application.parent_contact__c::VARCHAR = tours_agg.splash_parent_id
	GROUP BY
		1
		, 2
)
, unmatched_parent_checkins AS (
	SELECT
		parent_id AS unmatched_parent_id
	FROM
		raw_data_science.raw_unmatched_parent_checkins
)
, best_offer AS (
	SELECT 
		first_offer_date
		, uniform_ordered_date
		, student.id
		, students_home__latitude__s
		, students_home__longitude__s
		, grade
		, accepted_school
		, CASE WHEN grade SIMILAR TO ('5|6') THEN 1 ELSE 0 END AS summer_intensive
		, CASE
			WHEN attended_five.sa_scholar_id IS NOT NULL THEN 1
			ELSE 0
		END AS yield
		, datediff(DAY, first_offer_date, '{date}'::DATE) AS days_since_offer
		, CASE
			WHEN uniform_ordered_date <= '{date}'::DATE THEN 1
			ELSE 0
		END 
			AS uniform_ordered
		, CASE
			WHEN accepted_first_rank = 1 THEN 1
			ELSE 0
		END AS accepted_first_rank
		, CASE
			WHEN had_enrolled_sib SIMILAR TO ('Sibling Attending Same School|Sibling Attending Co-Located School|Child of Staff') 
		THEN 1
			ELSE 0
		END AS had_enrolled_sib
		, CASE 
			WHEN ell_status = 'Yes' THEN 1
			ELSE 0
		END AS ell_status
		, CASE
			WHEN homeless_status IS TRUE THEN 1
			ELSE 0
		END AS homeless_status
		, orientation_rsvp
		, CASE
				WHEN orientation_rsvp = 1
				OR orientation_checkin = 1 THEN 1
				ELSE 0
			END AS total_orientation_rsvps
			, CASE
					WHEN unmatched_parent_checkins.unmatched_parent_id IS NOT NULL THEN 1
				ELSE orientation_checkin
			END AS orientation_checkin
			, virtual_event_attended
			, in_person_event_attended
			, new_family_day
			, community_support_day
			, recruiter_first_name || ' ' || recruiter_last_name AS recruiter_full_name
			, CASE
					WHEN verified_parent IN ('Verified') THEN 'Parent Referral'
				WHEN lower(recruiter_first_name) LIKE '%quick%' THEN 'Field Team Quick App'
				WHEN recruiter_full_name IN (
						'Claire Leka', 'Claire Leika', 'Claire Lake', 'Claire Laka', 'Alexandra Kinderman', 'Alex Kinderman', 'Angela Johnson', 'Angela Jhonson', 'Santy Barrera Claire Leka', 'Wilmer Cabral', 'Santy Barrera', 'Laura Anderson', 'Marielis Perez', 'Fatima Akindele', 'Johanna Garcia', 'Huda Ali', 'Fatimat Akindele', 'Latoya Dakins', 'Dorrant Linton', 'Lefran Pierre', 'Naklah Saleh', 'Ralph Poinvil'
				) THEN 'Field Team Full App'
				WHEN utm_medium IN ('recruiter') THEN 'Field Team Full App'
				WHEN utm_source IN ('scholar_recruitment')
					AND utm_medium IN (
							'print', 'adpartnereblasts', 'MommyPoppins'
					) THEN 'Field Team Full App'
					WHEN utm_source IN ('scholar_recruitment')
						AND utm_medium IN (
								'in_person_tours', 'in_person_tour', 'open_house', 'virtual_info_sessions'
						) THEN 'Field Team Event Link'
						WHEN utm_source IN ('schoar_recruitment') THEN 'Field Team Event Link'
						WHEN utm_source IN ('scholar-recruitment')
							AND utm_medium IN ('eztext') THEN 'Field Team Event Link'
							WHEN utm_source IN ('school_tours')
								AND utm_medium IN ('tour_brochure') THEN 'Field Team Event Link'
								WHEN utm_source IN (
										'braze', 'emma'
								)
									OR utm_medium IN ('email')
										OR utm_campaign LIKE '%sms%'
										OR utm_campaign IN ('email') THEN 'Email/SMS'
										WHEN utm_source IN ('bing')
											AND utm_medium IN ('cpc') THEN 'Bing Branded Search'
											WHEN utm_source IN ('FM_FB_IG') THEN 'Paid Meta'
											WHEN utm_source IN ('niche') THEN 'Niche'
											WHEN utm_source IN ('GreatSchools') THEN 'Great Schools'
											WHEN utm_source IN ('offline') THEN 'Offline'
											WHEN utm_medium IN (
													'social', 'organic-social'
											) THEN 'Organic Social'
											WHEN utm_source IN ('parent_referral_program') THEN 'Parent Referral'
											WHEN utm_source IN ('Bitly')
												AND utm_medium IN ('Referral') THEN 'Parent Referral'
												WHEN utm_source IN ('tatari_streaming') THEN 'Tatari'
												WHEN utm_source IN ('thesis') THEN 'Thesis'
												WHEN utm_source IN ('FM_TIKTOK') THEN 'TikTok'
												WHEN utm_source IN (
														'faq-page', 'app-dashboard'
												) THEN 'Website'
												WHEN utm_source IN ('google')
													AND utm_medium IN ('cpc')
														AND (
																utm_campaign IN ('Brand')
																OR utm_campaign LIKE '%|Brand|%'
														) THEN 'Google Branded Search'
														WHEN utm_source IN ('google')
															AND utm_medium IN ('cpc')
																AND (
																		utm_campaign IN ('Scholar')
																		OR utm_campaign LIKE '%|NB|%'
																		OR utm_campaign LIKE '05052023'
																) THEN 'Google Non Brand Search'
																WHEN utm_source IN ('google')
																	AND utm_medium IN ('cpc')
																		AND utm_campaign LIKE '%|PerformanceMax|%' THEN 'Google Performance Max'
																		WHEN utm_source IN ('google')
																			AND utm_medium IN ('cpc')
																				AND utm_campaign LIKE '%|Discovery|%' THEN 'Google Discovery'
																				WHEN utm_source IN ('google')
																					AND utm_medium IN ('cpc')
																						AND utm_campaign LIKE '%|Video|%' THEN 'Google Video - YouTube'
																						WHEN utm_source IN ('google')
																							AND utm_medium IN ('cpc') THEN 'Google Non Brand Search'
																							ELSE 'Organic / No Tracking'
																						END AS utm_source_bucketing
																					FROM
																							student
																					INNER JOIN student_contact
	ON
																							student.id = student_contact.student
																					INNER JOIN application
	ON
																							student.id = application.student__c
																					INNER JOIN lottery
	ON
																							student.best_ever_cy_assignment__c = lottery.id
																						AND application.id = lottery.round_application__c
																					LEFT JOIN account
	ON
																							account.id = lottery.account_school__c
																					LEFT JOIN hed
	ON
																							application.id = hed.app_round_application__c
																						AND student.accepted_first_rank = hed.program_rank__c
																					LEFT JOIN referral
	ON
																							application.name = referral.application_id
																					LEFT JOIN attended_five
	ON
																							student_contact.esd_id = attended_five.sa_scholar_id
																					LEFT JOIN parent_contact
	    ON
																							parent_contact.parent_id = application.parent_contact__c
																					LEFT JOIN yield_probability_tours_status
		ON
																							application.name = yield_probability_tours_status.application_id
																					LEFT JOIN unmatched_parent_checkins 
		ON
																							parent_contact.parent_id = unmatched_parent_checkins.unmatched_parent_id
																					WHERE
																							utm_source_bucketing != 'Field Team Quick App'
																					AND first_offer_date <= '{date}'::DATE
)
SELECT
	*
	--count(*)
FROM
	best_offer
	WHERE NOT (first_offer_date < '2023-05-01' AND accepted_school IN ('SA Queens Village', 'SA Norwood', 'SA Sheepshead Bay')) ;