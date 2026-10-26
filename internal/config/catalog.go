package config

// Catalog lists the Centric endpoints centric-fetch knows. The config picks from it by name.
//
// Page size (default 50): a full fetch is bound by request count, not data volume, so
// high-volume endpoints use 500 where that's been measured to work. Heavy payloads (styles:
// ~3 s per 50) stay at 50 to keep responses within server limits.
var Catalog = []Endpoint{
	{Name: "seasons", Path: "seasons", APIVersion: "v2", CountPath: "count/Season"},
	{Name: "styles", Path: "styles", APIVersion: "v2", CountPath: "count/Style"},
	{Name: "materials", Path: "materials", APIVersion: "v2", CountPath: "count/Material"},
	{Name: "colorways", Path: "colorways", APIVersion: "v2", CountPath: "count/Colorway"},
	{Name: "collections", Path: "collections", APIVersion: "v2", CountPath: "count/Collection"},
	{Name: "category1s", Path: "category1s", APIVersion: "v2", CountPath: "count/Category1"},
	{Name: "category2s", Path: "category2s", APIVersion: "v2", CountPath: "count/Category2"},
	{Name: "sizes", Path: "product_sizes", APIVersion: "v2", CountPath: "count/ProductSize"},
	{Name: "size_ranges", Path: "size_ranges", APIVersion: "v2", CountPath: "count/SizeRange"},
	{Name: "size_charts", Path: "size_charts", APIVersion: "v2", CountPath: "count/SizeChart"},
	{Name: "size_chart_revisions", Path: "size_chart_revisions", APIVersion: "v2", CountPath: "count/SizeChartRevision"},
	{Name: "size_chart_dimensions", Path: "size_chart_dimensions", APIVersion: "v2", CountPath: "count/SizeChartDimension", Limit: 500},
	{Name: "apparel_dimensions", Path: "apparel_dimensions", APIVersion: "v2", CountPath: "count/ApparelDimension"},
	{Name: "bom_masters", Path: "apparel_boms", APIVersion: "v2", CountPath: "count/ApparelBOM"},
	{Name: "bom_subtypes", Path: "apparel_bom_subtypes", APIVersion: "v2", CountPath: "count/ApparelBOMSubtype"},
	{Name: "boms", Path: "apparel_bom_revisions", APIVersion: "v3", CountPath: "count/ApparelBOMRevision"},
	{Name: "bom_sections", Path: "bom_section_definitions", APIVersion: "v2", CountPath: "count/BOMSectionDefinition"},
	{Name: "bom_lines", Path: "part_materials", APIVersion: "v3", CountPath: "count/PartMaterial", Limit: 500},
	{Name: "supplier_quote_masters", Path: "supplier_items", APIVersion: "v2", CountPath: "count/SupplierItem"},
	{Name: "supplier_quotes", Path: "supplier_item_revisions", APIVersion: "v2", CountPath: "count/SupplierItemRevision", Limit: 500},
	{Name: "suppliers", Path: "suppliers", APIVersion: "v2", CountPath: "count/Supplier"},
	{Name: "factories", Path: "factories", APIVersion: "v2", CountPath: "count/Factory"},
	{Name: "color_specifications", Path: "color_specifications", APIVersion: "v2", CountPath: "count/ColorSpecification"},
	{Name: "compositions", Path: "compositions", APIVersion: "v2", CountPath: "count/Composition"},
	{Name: "material_compositions", Path: "material_compositions", APIVersion: "v2", CountPath: "count/MaterialComposition"},
	{Name: "care_and_compositions", Path: "care_and_compositions", APIVersion: "v2", CountPath: "count/CareAndComposition"},
	{Name: "care_and_composition_revisions", Path: "care_and_composition_revisions", APIVersion: "v2", CountPath: "count/CareAndCompositionRevision"},
	{Name: "care_symbols", Path: "care_symbols", APIVersion: "v2", CountPath: "count/CareSymbol"},
	{Name: "countries", Path: "countries", APIVersion: "v2", CountPath: "count/Country"},
	{Name: "shipping_ports", Path: "shipping_ports", APIVersion: "v2", CountPath: "count/ShippingPort"},
	{Name: "lookup_items", Path: "lookup_items", APIVersion: "v2", CountPath: "count/LookupItem"},
	{Name: "lookup_item_subtypes", Path: "lookup_item_subtypes", APIVersion: "v2", CountPath: "count/LookupItemSubtype"},
	{Name: "material_types", Path: "material_types", APIVersion: "v2", CountPath: "count/MaterialType"},
	{Name: "style_types", Path: "style_types", APIVersion: "v2", CountPath: "count/StyleType"},
	{Name: "product_sources", Path: "product_sources", APIVersion: "v2", CountPath: "count/ProductSource"},
	{Name: "routings", Path: "routings", APIVersion: "v2", CountPath: "count/Routing"},
	{Name: "routing_items", Path: "routing_items", APIVersion: "v2", CountPath: "count/RoutingItem"},
	{Name: "routing_revisions", Path: "routing_revisions", APIVersion: "v2", CountPath: "count/RoutingRevision"},
	{Name: "documents", Path: "documents", APIVersion: "v2", CountPath: "count/Document"},
	{Name: "document_revisions", Path: "document_revisions", APIVersion: "v2", CountPath: "count/DocumentRevision"},
	{Name: "duty_rates", Path: "duty_rates", APIVersion: "v2", CountPath: "count/DutyRate"},
	{Name: "users", Path: "users", APIVersion: "v2", CountPath: "count/User"},
}

const defaultLimit = 50

func catalogEndpoint(name string) (Endpoint, bool) {
	for _, e := range Catalog {
		if e.Name == name {
			if e.Limit == 0 {
				e.Limit = defaultLimit
			}
			return e, true
		}
	}
	return Endpoint{}, false
}
