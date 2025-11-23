WITH trading_information AS (
	Select 
		COUNT(DISTINCT t.trader_id) AS total_trading_partners,
		SUM(tt.value) AS all_time_trade_value,
		SUM(tt.balance_direction) AS all_time_trade_balance,
	From
		Trades t
	LEFT JOIN
		TRADE_TRANSACTIONS tt ON tt.caravan_id = t.caravan_id
)
Select
	ti.total_trading_partners,
	ti.all_time_trade_value,
	ti.all_time_trade_balance,
	JSON_OBJECT (
		'civilization_data', JSON_OBJECT (
			'civilization_trade_data', (
				Select JSON_ARRAY_AGG(
					JSON_OBJECT (
						'civilization_type', c.civilization_type,
						'total_caravans', COUNT(DISTINCT c.caravan_id),
						'total_trade_value', SUM(tt.value),
						'trade_balance', SUM(CASE WHEN tt.balance_direction = 'IN' THEN tt.value ELSE -tt.value END),
						'trade_relationship', de.relationship_change,
						'diplomatic_correlation', CORR(CASE WHEN de.relationship_change = 'Favorable' THEN 1 ELSE 0 END, de.outcome::DECIMAL)
						JSON_OBJECT (
							'caravan_ids', (
								SELECT JSON_ARRAY_AGG(c.caravan_id)
								FROM Caravans c
								WHERE c.caravan_id = tt.caravan_id
							)
						)
					)
				)
				From 
					CARAVANS c
				LEFT JOIN 
					TRADE_TRANSACTIONS tt  ON c.caravans_id = tt.caravans_id
				LEFT JOIN
					DIPLOMATIC_EVENTS de ON c.caravan_id = de.caravan_id
				GROUP BY
					c.civilization_type
			)
		)
		'critical_import_dependencies', JSON_OBJECT (
			'resource_dependency', (
				Select JSON_ARRAY_AGG(
					JSON_OBJECT (
						'material_type', cg.material_type,
						-- 'dependency_score', 
						'total_imported', SUM(CASE WHEN cg.type = 'Import' THEN cg.quantity ELSE 0 END),
						'import_diversity', COUNT(DISTINCT CASE WHEN cg.type = 'Import' THEN cg.goods_id END),
						JSON_OBJECT (
						cg.caravan_id
							'resource_ids', (
								SELECT JSON_ARRAY_AGG(ft.resource_id)
								FROM 
									CARAVAN_GOODS cg
								LEFT JOIN
									CARAVANS c ON cg.caravan_id = c.caravan_id
								LEFT JOIN
									FORTRESS_RESOURCES fr ON c.fortress_id = fr.fortress_id
								GROUP BY cg.caravan_id
							)
						)
					)
				)
				FROM
					CARAVAN_GOODS cg
				GROUP BY
					cg.material_type
			)
		)
		'export_effectiveness', JSON_OBJECT (
			'export_effectiveness', (
				SELECT JSON_ARRAY_AGG (
					JSON_OBJECT (
						'workshop_type', w.type,
						'product_type', p.type,
						'export_ratio', ROUND(SUM(CASE WHEN cg.type = 'Export' THEN cg.quantity ELSE 0 END))::DECIMAL / NULLIF(SUM(cg.quantity)), 0), 2),
						'avg_markup', AVG(cg.price_fluctuation::DECIMAL),
						JSON_OBJECT (
							'workshop_ids', (
								SELECT JSON_ARRAY_AGG(workshop_id)
								FROM WORKSHOP w
								WHERE w.workshop_id = p.workshop_id
							)
						)
					)
				)
				FROM
					WORKSHOP w
				LEFT JOIN
					PRODUCTS p ON w.workshop_id = p.workshop_id
				LEFT JOIN
					CARAVAN_GOODS cg ON cg.original_product_id = p.product_id
				GROUP BY
					w.type, p.type
			)
		)
		'trade_timeline', JSON_OBJECT (
			'trade_growth', (
				SELECT JSON_ARRAY_AGG (
					JSON_OBJECT (
						'year', EXTRACT(YEAR FROM tt.date),
						'quarter', EXTRACT(QUARTER FROM tt.date),
						'quarterly_value', SUM(tt.value),
						'quarterly_balance', SUM(CASE WHEN tt.balance_direction = 'IN' THEN tt.value ELSE -tt.value END),
						'trade_diversity', COUNT(DISTINCT cg.goods_id)
					)
				)
				FROM 
					TRADE_TRANSACTIONS tt
				LEFT JOIN
					CARAVAN_GOODS cg ON cg.caravan_id = tt.caravan_id
				GROUP BY 
					EXTRACT(YEAR FROM tt.date), EXTRACT(QUARTER FROM tt.date)
			)
		)	
	)
