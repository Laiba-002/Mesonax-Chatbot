"""
Knowledge Graph Schema Definition
Based on Manufacturing Hierarchy
"""

SCHEMA = {
    "nodes": {
        "Group": {
            "primary_key": "GroupCode",
            "view": "view_GetAllGroups",
            "properties": ["GroupCode", "GroupName", "Description", "Status", "UserCode"],
            "level": 1
        },
        "Plant": {
            "primary_key": "PlantCode",
            "view": "View_GetAllPlants",
            "properties": ["PlantCode", "GroupCode", "PlantName", "Status", "Description", "Varient", "TimeZoneName"],
            "level": 2
        },
        "Line": {
            "primary_key": "LineId",
            "view": "View_GetAllLines",
            "properties": ["LineId", "LineName", "LineCode", "PlantCode", "Capacity", "IsActive"],
            "level": 3
        },
        "Machine": {
            "primary_key": "MachineCode",
            "view": "View_GetAllMachines",
            "properties": ["MachineId", "MachineCode", "MachineName", "LineId", "PlantCode", "UserCode"],
            "level": 4
        },
        "Product": {
            "primary_key": "ProductId",
            "view": "View_Products",
            "properties": ["ProductId", "ProductName", "Description", "PlantCode", "UserCode"],
            "level": 1
        },
        "ProductionPlan": {
            "primary_key": "OrderId",
            "view": "View_ProductionPlan",
            "properties": ["OrderId", "ProductionOrderCode", "ProductId", "PlannedQty", "StartDate", "EndDate", "UserCode", "PlantCode"],
            "level": 5
        },

        "POOperation": {
            "primary_key": "OperationId",
            "view": "View_POOperations",
            "properties": ["OperationId", "OrderId", "OperationName", "ProductName", "QtyPlanned", "StartTime", "EndTime", "PlantCode", "StatusName", "UOMId", "MachineCode",
                           "OperationQTY", "UserCode", "Type_Name", "ProductId"],
            "level": 6
        },
        "OperationConsumption": {
            "primary_key": "ConsumptionId",
            "view": "view_PO_OperationConsumption",
            "properties": ["InfoId", "OrderId", "OperationId", "LotCode", "Date", "InfoCreatedby", "CreatedOn", "InfolsDeleted", "Remarks", "ConsumptionId", "Material", "Qouta", "Quantity", "ProductId"],
            "level": 7
        },

        "ProductionOutput": {
            "primary_key": "ProductionOutputId",
            "view": "View_ProductionPlan_Output",
            "properties": ["ProductionOutputId", "OperationId", "BatchCode", "MachineCode", "ProductId", "OrderId", "UserCode", "StartDate", "EndDate", "IsGolden", "ProducedQty", "QcPassed", "PlantCode"
                           ],
            "level": 8
        },
        "ProductionBatch": {
            "primary_key": "LotCode",
            "view": "View_ProductionPlan_Output_Batches",
            "properties": ["ProductionOutputId", "MaterialConsumptionId", "LotCode", "OperationId"],
            "level": 9
        },
        "Employees": {
            "primary_key": "EmployeeCode",
            "view": "View_Employees",
            "properties": ["Id", "EmployeeCode", "EmployeeNo", "FirstName", "LastName", "Email", "Designation", "DateOfBirth", "DateOfJoining", "Nationality", "EmployeeImage", "PhoneNumber", "Description", "Status", "ReportTo", "CreatedBy", "UpdatedBy", "CreatedOn", "UpdatedOn", "IsActive", "PlantCode", "GroupCode", "OldUserId", "EmpInfo_Id", "RoleId"],
            "level": 1
        },
        "Roles": {
            "primary_key": "RoleId",
            "view": "View_Roles",
            "properties": ["RoleId", "RoleName", "PlantCode", "GroupCode", "Status"],
            "level": 1
        },
        "WasteLosses": {
            "primary_key": "LossId",
            "view": "View_WasteLosses",
            "properties": ["MachineCode", "OperationId", "OrderId", "StartTime", "Duration", "AllReasons", "AllReasonIds", "Remarks", "LossId", "ReasonTreeNodeId", "TreeTypeId", "WasteQuantity", "CategoryName", "CategoryId", "GrossProduced", "IdealProduced"],
            "level": 7
        },
        "DowntimeLosses": {
            "primary_key": "LossId",
            "view": "View_DowntimeLosses",
            "properties": ["MachineCode", "OperationId", "OrderId", "StartTime", "EndTime", "Duration", "AllReasons", "AllReasonIds", "Remarks", "LossId", "ReasonTreeNodeId", "TreeTypeId", "CategoryName", "CategoryId"],
            "level": 4
        },
        "ProductionPlan_RelatedTasks": {
            "primary_key": "TaskId",
            "view": "View_ProductionPlan_RelatedTasks",
            "properties": ["TaskId", "OrderId", "TaskName", "OperationId", "Status", "TaskType", "ActionType", "AssignTo", "StartDate", "ToTillDate", "RevisedDate", "NoOfMisses", "StatusCompleted", "StatusVerification", "NewAssignee", "Remarks", "PlantCode", "CreatedBy", "UpdatedBy", "CanBeVerify", "Priority", "Verify"],
            "level": 6
        },
        "ChecklistExecution": {
            "primary_key": "ChecklistId",
            "view": "View_ChecklistExecution",
            "properties": ["ChecklistExecutionId", "ChecklistId", "ScreenName", "ChecklistName", "PlantCode", "ExecutionDate", "OperationId", "ProductionOutputId", "ChecklistType", "VerifiedRemarks", "VerifiedOn"],
            "level": 7
        },
        "ChecklistExecutionDetail": {
            "primary_key": "ChecklistExecutionId",
            "view": "View_ChecklistExecutionDetail",
            "properties": ["ChecklistExecutionId", "ChecklistGroupName", "QuestionType", "Question", "GoodExample", "BadExample", "Result", "Remarks"],
            "level": 8
        },
        "ProductionPlan_Parameters": {
            "primary_key": "VariableId",
            "view": "View_ProductionPlan_Parameters",
            "properties": ["VariableId", "OrderId", "OperationId", "ProductId", "Result", "ResultOn", "LRL", "LSL", "LWL", "Target", "UWL", "USL", "URL", "ParameterName", "ParameterType", "DataTypeId", "DataType"],
            "level": 7
        },
        "Vendors": {
            "primary_key": "VendorId",
            "view": "View_GetAllVendors",
            "properties": ["VendorId", "VendorName", "Email", "PlantCode", "UserCode"],
            "level": 1
        },
        "ProductMachineSpeed": {
            "primary_key": "MappingId",
            "view": "View_ProductMachineSpeed",
            "properties": ["MappingId", "MachineCode", "ProductId", "Speed", "VarientId"],
            "level": 4

        }
    },

    "relationships": [
        # Level 1-2: Group to Plant
        {
            "name": "HAS_PLANT",
            "from_node": "Group",
            "to_node": "Plant",
            "from_property": "GroupCode",
            "to_property": "GroupCode",
            "description": "Group has Plant relationship"
        },

        # Level 2-3: Plant to Line
        {
            "name": "HAS_LINE",
            "from_node": "Plant",
            "to_node": "Line",
            "from_property": "PlantCode",
            "to_property": "PlantCode",
            "description": "Plant has Line relationship"
        },

        # Level 3-4: Line to Machine
        {
            "name": "HAS_MACHINE",
            "from_node": "Line",
            "to_node": "Machine",
            "from_property": "LineId",
            "to_property": "LineId",
            "description": "Line has Machine relationship"
        },

        # Level 5: ProductionPlan Relationships
        {
            "name": "PRODUCES_PRODUCT",
            "from_node": "ProductionPlan",
            "to_node": "Product",
            "from_property": "ProductId",
            "to_property": "ProductId",
            "description": "ProductionPlan produces Product relationship"
        },
        {
            "name": "PLANNED_AT_PLANT",
            "from_node": "ProductionPlan",
            "to_node": "Plant",
            "from_property": "PlantCode",
            "to_property": "PlantCode",
            "description": "ProductionPlan planned at Plant relationship"
        },


        # Level 6: ProductionPlanDetail Relationships
        {
            "name": "HAS_DETAIL",
            "from_node": "ProductionPlan",
            "to_node": "POOperation",
            "from_property": "OrderId",
            "to_property": "OrderId",
            "description": "ProductionPlan has Detail relationship"
        },


        # Level 7: POOperation Relationships
        {
            "name": "HAS_OPERATION",
            "from_node": "Product",
            "to_node": "POOperation",
            "from_property": "ProductId",
            "to_property": "ProductId",
            "description": "Product has Operation relationship"
        },
        {
            "name": "EXECUTED_ON_MACHINE",
            "from_node": "POOperation",
            "to_node": "Machine",
            "from_property": "MachineCode",
            "to_property": "MachineCode",
            "description": "POOperation executed on Machine relationship"
        },

        # Level 8: OperationConsumption Relationships
        {
            "name": "CONSUMES_MATERIAL",
            "from_node": "POOperation",
            "to_node": "OperationConsumption",
            "from_property": "OperationId",
            "to_property": "OperationId",
            "description": "POOperation consumes Material relationship"
        },

        # Level 9: ProductionBatch Relationships
        {
            "name": "PRODUCES_OUTPUT",
            "from_node": "POOperation",
            "to_node": "ProductionOutput",
            "from_property": "OperationId",
            "to_property": "OperationId",
            "description": "POOperation produces Output relationship"
        },
        {
            "name": "PRODUCES_BATCH",
            "from_node": "ProductionOutput",
            "to_node": "ProductionBatch",
            "from_property": "ProductionOutputId",
            "to_property": "ProductionOutputId",
            "description": "ProductionOutput produces Batch relationship"
        },

        # Employee and Role relationships
        {
            "name": "HAS_ROLE",
            "from_node": "Employees",
            "to_node": "Roles",
            "from_property": "RoleId",
            "to_property": "RoleId",
            "description": "Employee has Role relationship"
        },

        # Employee and ProductionPlan_RelatedTasks relationships
        {
            "name": "ASSIGNED_TO_TASK",
            "from_node": "Employees",
            "to_node": "ProductionPlan_RelatedTasks",
            "from_property": "EmployeeCode",
            "to_property": "AssignTo",
            "description": "Employee assigned to Task relationship"
        },

        # ProductionPlan_RelatedTasks and POOperations relationships
        {
            "name": "RELATES_TO_OPERATION",
            "from_node": "ProductionPlan_RelatedTasks",
            "to_node": "POOperation",
            "from_property": "OperationId",
            "to_property": "OperationId",
            "description": "Task relates to Operation relationship"
        },

        # DowntimeLosses and Machine relationships
        {
            "name": "CAUSES_DOWNTIME",
            "from_node": "DowntimeLosses",
            "to_node": "Machine",
            "from_property": "MachineCode",
            "to_property": "MachineCode",
            "description": "DowntimeLosses causes Downtime relationship"
        },

        # WasteLosses and POOperation relationships
        {
            "name": "GENERATES_WASTE",
            "from_node": "WasteLosses",
            "to_node": "POOperation",
            "from_property": "OperationId",
            "to_property": "OperationId",
            "description": "WasteLosses generates Waste relationship"
        },

        # ChecklistExecution and POOperation relationships
        {
            "name": "EXECUTES_FOR_OPERATION",
            "from_node": "ChecklistExecution",
            "to_node": "POOperation",
            "from_property": "OperationId",
            "to_property": "OperationId",
            "description": "ChecklistExecution for Operation relationship"
        },

        # ProductionOutput and ChecklistExecution relationships
        {
            "name": "HAS_CHECKLIST",
            "from_node": "ProductionOutput",
            "to_node": "ChecklistExecution",
            "from_property": "ProductionOutputId",
            "to_property": "ProductionOutputId",
            "description": "ProductionOutput has Checklist relationship"
        },

        # ChecklistExecution and ChecklistExecutionDetail relationships
        {
            "name": "HAS_CHECKLIST_DETAIL",
            "from_node": "ChecklistExecution",
            "to_node": "ChecklistExecutionDetail",
            "from_property": "ChecklistExecutionId",
            "to_property": "ChecklistExecutionId",
            "description": "ChecklistExecution has Detail relationship"
        },

        # ProductionPlan_Parameters and POOperation relationships
        {
            "name": "PARAMETERS_FOR_OPERATION",
            "from_node": "ProductionPlan_Parameters",
            "to_node": "POOperation",
            "from_property": "OperationId",
            "to_property": "OperationId",
            "description": "Parameters for Operation relationship"
        },

        # Vendors and Product relationships
        {
            "name": "SUPPLIES_PRODUCT",
            "from_node": "Vendors",
            "to_node": "Product",
            "from_property": "VendorId",
            "to_property": "VendorId",
            "description": "Vendor supplies Product relationship"
        },

        # ProductMachineSpeed and Machine relationships
        {
            "name": "DEFINES_SPEED_FOR",
            "from_node": "ProductMachineSpeed",
            "to_node": "Machine",
            "from_property": "MachineCode",
            "to_property": "MachineCode",
            "description": "ProductMachineSpeed defines speed for Machine relationship"
        }
    ]

}


# NLP Entity Mappings
ENTITY_KEYWORDS = {
    "Group": ["group", "groups", "organization", "division", "business unit", "corporate", "enterprise"],
    "Plant": ["plant", "plants", "facility", "facilities", "factory", "factories", "site", "sites", "location", "locations", "manufacturing plant"],
    "Line": ["line", "lines", "production line", "assembly line", "manufacturing line", "production lines", "assembly lines"],
    "Machine": ["machine", "machines", "equipment", "asset", "unit", "units", "device", "devices", "machinery", "apparatus", "tool", "tools", "station", "stations", "workstation", "prone to faults", "fault prone", "unreliable"],
    "Product": ["product", "products", "item", "items", "sku", "part", "parts", "component", "components", "material", "materials", "goods", "finished goods"],
    "ProductionPlan": ["plan", "plans", "production plan", "schedule", "schedules", "production schedule", "manufacturing plan", "planning", "production planning"],
    "ProductionPlanDetail": ["detail", "details", "plan detail", "schedule detail"],
    "POOperation": ["operation", "operations", "work order", "work orders", "job", "jobs", "process", "processes", "task execution", "production operation", "manufacturing operation"],
    "OperationConsumption": ["consumption", "material consumption", "usage", "material usage", "resource consumption", "consumed material", "input material", "raw material usage"],
    "ProductionBatch": ["batch", "batches", "lot", "lots", "lot number", "batch number", "production batch", "production lot"],
    "Employees": ["employee", "employees", "worker", "workers", "staff", "personnel", "operator", "operators", "technician", "technicians", "team member", "workforce"],
    "Roles": ["role", "roles", "position", "positions", "job title", "designation", "function"],
    "WasteLosses": ["waste", "losses", "waste loss", "scrap", "scrapped", "rejected", "defect", "defects", "defective", "rejection", "rejections", "rejection rate", "scrap rate", "waste rate", "quality loss", "quality losses", "rework", "rejects", "discarded", "spoilage"],
    "DowntimeLosses": ["downtime", "downtime loss", "machine downtime", "breakdown", "breakdowns", "failure", "failures", "stoppage", "stoppages", "outage", "outages", "unavailable", "unavailability", "idle time", "lost time", "unplanned downtime", "planned downtime", "maintenance downtime", "fault", "faults", "prone to faults", "fault-prone"],
    "ProductionPlan_RelatedTasks": ["task", "tasks", "production task", "related task", "assignment", "assignments", "action item", "action items", "work item", "to-do"],
    "ChecklistExecution": ["checklist", "checklists", "checklist execution", "quality checklist", "inspection checklist", "quality check", "quality checks", "inspection", "inspections", "audit", "audits", "verification"],
    "ChecklistExecutionDetail": ["checklist detail", "checklist item", "inspection item", "inspection detail", "check item", "verification item", "audit detail"],
    "ProductionPlan_Parameters": ["parameter", "parameters", "production parameter", "process parameter", "process parameters", "control parameter", "quality parameter", "specifications", "spec", "specs", "tolerance", "tolerances", "limits", "control limits"],
    "Vendors": ["vendor", "vendors", "supplier", "suppliers", "provider", "providers", "third party", "external supplier", "source"],
    "ProductMachineSpeed": ["speed", "machine speed", "production speed", "speed setting", "production rate", "cycle time", "throughput", "capacity", "rate"]
}

RELATIONSHIP_KEYWORDS = {
    "hierarchy": ["structure", "organization", "hierarchy", "belongs to", "contains"],
    "production": ["produces", "manufactures", "makes", "creates"],
    "consumption": ["consumes", "uses", "requires", "needs"],
    "execution": ["executes", "runs on", "performed on"]
}

AGGREGATION_KEYWORDS = {
    "count": ["count", "number of", "how many", "total"],
    "sum": ["sum", "total", "aggregate"],
    "average": ["average", "mean", "avg"],
    "max": ["maximum", "max", "highest", "most"],
    "min": ["minimum", "min", "lowest", "least"]
}
