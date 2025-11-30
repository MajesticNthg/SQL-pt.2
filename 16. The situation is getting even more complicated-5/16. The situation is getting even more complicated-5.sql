WITH creature_attacks_information AS (
	SELECT
		c.type AS creature_type
		COUNT(ca.attack_id) AS recorded_attacks,
		COUNT(DISTINCT CASE WHEN ca.outcome = 'Unfavorable' THEN ca.attack_id ELSE 0 END) AS outcome_unfavorable,
		COUNT(DISTINCT CASE WHEN ca.outcome = 'Favorable' THEN ca.attack_id ELSE 0 END) AS outcome_favorable,
		CASE WHEN c.threat_level < 2 THEN 'LOW' WHEN c.threat_level > 2 AND c.threat_level < 4 THEN 'Moderate' WHEN c.threat_level > 4 THEN 'HIGH' ELSE 'UNKNOWN' END AS current_threat_level
	FROM 
		CREATURE_ATTACKS ca
	JOIN
		CREATURE c ON c.creature_id = ca.creature_id
),
zone_information AS (
	SELECT
		l.location_id AS location_id
		l.zone_id AS zone_id
		l.name AS zone_name,
		l.fortification_level AS fortification_level,
		ROUND((l.wall_integrity * l.trap_density * l.choke_points * 0.85, 0) * 100, 2) AS vulnerability_score	
	FROM
		LOCATIONS l
),
defense_information AS (
	SELECT
		l.location_id AS location_id
		l.zone_type AS defense_type,
		ROUND(l.trap_density * l.choke_points * 0.75, 0) * 100, 2) AS effectiveness_rate,
		AVG(ca.enemy_casualties) AS avg_enemy_casualties
	FROM
		LOCATION l
	JOIN
		CREATURE_ATTACKS ca ON ca.location_id = l.location_id
),
military_readiness_assessment_information AS (
	SELECT
		ms.squad_id AS squad_id,
		ms.name AS squad_name,
		COUNT(DISTINCT CASE WHEN sm.exit_reason IS NULL THEN sm.dwarf_id END) AS active_members,
		AVG(ds.level) AS avg_combat_skill,
		ROUND((COUNT(CASE WHEN so.status = 'Complete' THEN 1 ELSE 0 END))::DECIMAL / NULLIF (COUNT(so.operation_id, 0)), 1) AS combat_effectiveness,
	FROM
		MILITARY_SQUADS ms
	JOIN
		SQUAD_MEMBERS sm ON sm.squad_id = ms.squad_id
	JOIN
		DWARF_SKILLS ds ON ds.dwarf_id = sm.dwarf_id
	JOIN
		SQUAD_OPERATION so ON so.squad_id = sm.squad_id
),
security_evolution_information AS (
	SELECT
		f.founded_year AS founded_year,
		COUNT(DISTINCT CASE WHEN ca.outcome = 'Unfavorable' THEN ca.attack_id ELSE 0 END) AS outcome_unfavorable,
		COUNT(DISTINCT CASE WHEN ca.outcome = 'Favorable' THEN ca.attack_id ELSE 0 END) AS outcome_favorable,
		COUNT(ca.attack_id) AS total_attack,
		COUNT(ca.casualties) AS casualties,
	FROM
		FORTRESS f
	JOIN
		CREATURE_ATTACKS ca ON f.location = ca.location_id
)
Select 
	SUM(cai.recorded_attacks) AS total_recorded_attacks,
	COUNT(DISTINCT ca.creature_id) AS unique_attackers,
	SUM(ROUND((cai.outcome_favorable::DECIMAL / NULLIF(cai.outcome_favorable + cai.outcome_unfavorable, 0)) * 100, 2)) AS overall_defense_success_rate,
	JSON_OBJECT (
		'threat_assessment', JSON_OBJECT (
			'current_threat_level', cai.current_threat_level
			'active_threats', (
				SELECT JSON_ARRAYAGG(
					JSON_OBJECT (
						'creature_type', cai.creature_type,
						'threat_level', c.threat_level,
						'last_sighting_date', MAX(cs.date),
						'distance_to_fortress', ct.distance_to_fortress,
						'estimated_numbers', c.estimated_population,
						'creature_ids', (
							SELECT JSON_ARRAYAGG(c.creature_id)
							FROM CREATURES c
							JOIN CREATURE_TERRITORIES ct ON c.creature_id = ct.creature_id
							WHEN cai.creature_type = c.type
						)
					)
				)
			)
		)
		FROM 
			creature_attacks_information cai
	
	
	
	JSON_OBJECT (
		'vulnerability_analysis', (
			SELECT JSON_ARRAYAGG(
				JSON_OBJECT (
					'zone_id', zi.zone_id,
					'zone_name', zi.zone_name,
					'vulnerability_score', zi.vulnerability_score,
					-- historical_breaches,
					'fortification_level', zi.fortification_level,
					'military_response_time', ca.military_response_time_minutes,
					'defense_coverage', JSON_OBJECT (
						'structure_ids', (
							SELECT JSON_ARRAYAGG(ca.defense_structures_used)
							FROM CREATURE_ATTACKS ca
							WHERE ca.location_id = zi.location_id
						), 
						'squad_ids' (
							SELECT JSON_ARRAYAGG(st.squad_id)
							FROM SQUAD_TRAINING st
							WHERE st.location_id = zi.location_id
						)
					)
					FROM zone_information zi
				)
			)
		)
	),
	
	JSON_OBJECT (
		'defense_effectiveness', (
			SELECT JSON_ARRAYAGG(
				JSON_OBJECT(
					'defense_type', di.defense_type,
					'effectiveness_rate', di.effectiveness_rate,
					'avg_enemy_casualties', di.avg_enemy_casualties,
					'structure_ids', (
						SELECT JSON_ARRAYAGG(ca.defense_structures_used)
						FROM CREATURE_ATTACKS ca
						WHERE ca.location_id = di.location_id
					)
				)
				FROM defense_information di
			)
		)
	),
	
	JSON_OBJECT (
		'military_readiness_assessment', (
			SELECT JSON_ARRAYAGG (
				JSON_OBJECT(
					'squad_id', mrai.squad_id,
					'squad_name', mrai.squad_name,
					-- 'readiness_score', 
					'active_members', mrai.active_members,
					'avg_combat_skill', ROUND(AVG(mrai.avg_combat_skill), 1),
					'combat_effectiveness', mrai.combat_effectiveness,
					'response_coverage', (
						SELECT JSON_ARRAYAGG (
							JSON_OBJECT (
								'zone_id', l.zone_id,
								'response_time', ca.military_response_time_minutes,
							)
						)
						FROM
							LOCATION l
						JOIN
							CREATURE_ATTACKS ca ON l.location_id = ca.location_id
					)
				)
			)
			FROM 
				military_readiness_assessment_information mrai
		)
	),
	
	JSON_OBJECT (
		'security_evolution', (
			SELECT JSON_ARRAYAGG(
				JSON_OBJECT (
					'year', EXTRACT(YEAR FROM sei.founded_year),
					'defense_success_rate', ROUND((sei.outcome_favorable::DECIMAL / NULLIF(sei.outcome_favorable + sei.outcome_unfavorable, 0)) * 100, 2),
					'total_attacks', sei.total_attack,
					'casualties', sei.casualties,
					'year_over_year_improvement' defense_rating - LAG(defense_rating) OVER (ORDER BY EXTRACT(YEAR FROM report_date)) 
				)
			)
			FROM
				security_evolution_information sei
			ORDER BY
				EXTRACT(YEAR FROM year) DESC
			LIMIT
				2
		)
	)
) AS security_analysis
	