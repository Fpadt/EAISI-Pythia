# EconTone specific Functions for General Analysis

fGetVW <- 
  function(pSYSTID, pView ){
    switch (pView,
            "DSCP" = fGetMARC(pSYSTID),
    )
  }

fGetMARC <-
  function(pSYSTID){
    fRead_and_Union(
      pSIDCLNT  = pSYSTID,
      pTable    = "MARC",
      pOptions  = list("LVORM = ''"),
      pFields   = list(
        #      "MANDT"                   , # Client                                                        X
        "MATNR"                   , # Material Number                                               X
        "WERKS"                   , # Plant                                                         X
        "LVORM"                   , # Flag Material for Deletion at Plant Level
        "MMSTA"                   , # Plant-Specific Material Status
        "DISMM"                   , # MRP Type
        "MMSTD"                   , # Date from Which the Plant-Specific Material Status Is Valid
        "PSTAT"                   , # Maintenance status
        "MAABC"                   , # ABC Indicator
        "DISPO"                   , # MRP Controller
        "PLIFZ"                   #, # Planned Delivery Time in Days
      )
    )
  }

fMARC_ALL <-
  function(){
    list(
#      "MANDT"                   , # Client                                                        X
      "MATNR"                   , # Material Number                                               X
      "WERKS"                   , # Plant                                                         X
      "PSTAT"                   , # Maintenance status
      "LVORM"                   , # Flag Material for Deletion at Plant Level
      "BWTTY"                   , # Valuation Category
      "XCHAR"                   , # Batch management indicator (internal)
      "MMSTA"                   , # Plant-Specific Material Status
      "MMSTD"                   , # Date from Which the Plant-Specific Material Status Is Valid
      "MAABC"                   , # ABC Indicator
#      "KZKRI"                   , # Indicator: Critical part
      "EKGRP"                   , # Purchasing Group
      "AUSME"                   , # Unit of issue
#      "DISPR"                   , # Material: MRP profile
      "DISMM"                   , # MRP Type
      "DISPO"                   , # MRP Controller
#      "KZDIE"                   , # Indicator: MRP controller is buyer (deactivated)
      "PLIFZ"                   , # Planned Delivery Time in Days
      "WEBAZ"                   , # Goods receipt processing time in days
      "PERKZ"                   , # Period Indicator
#      "AUSSS"                   , # Assembly scrap in percent
      "DISLS"                   , # Lot Sizing Procedure within Materials Planning
      "BESKZ"                   , # Procurement Type
      "SOBSL"                   , # Special procurement type
#      "MINBE"                   , # Reorder Point
      "EISBE"                   , # Safety Stock
      "BSTMI"                   , # Minimum Lot Size
      "BSTMA"                   , # Maximum Lot Size
      "BSTFE"                   , # Fixed lot size
      "BSTRF"                   , # Rounding value for purchase order quantity
#      "MABST"                   , # Maximum Stock Level
#      "LOSFX"                   , # Lot-Size-Independent Costs
      "SBDKZ"                   , # Dependent requirements ind. for individual and coll. reqmts
      "LAGPR"                   , # Storage Costs Percentage Code
      "ALTSL"                   , # Method for Selecting Alternative Bills of Material
      "KZAUS"                   , # Discontinuation indicator
      "AUSDT"                   , # Effective-Out Date
      "NFMAT"                   , # Follow-Up Material
#      "KZBED"                   , # Indicator for Requirements Grouping
#      "MISKZ"                   , # Mixed MRP indicator
      "FHORI"                   , # Scheduling Margin Key for Floats
#      "PFREI"                   , # Indicator: automatic fixing of planned orders
#      "FFREI"                   , # Release indicator for production orders
      "RGEKZ"                   , # Indicator: Backflush
      "FEVOR"                   , # Production Supervisor
#      "BEARZ"                   , # Processing time
#     "RUEZT"                   , # Setup and teardown time
#      "TRANZ"                   , # Interoperation time
#      "BASMG"                   , # Base quantity
      "DZEIT"                   , # In-house production time
      "MAXLZ"                   , # Maximum Storage Period
      "LZEIH"                   , # Unit for maximum storage period
#      "KZPRO"                   , # Indicator: withdrawal of stock from production bin
#      "GPMKZ"                   , # Indicator: Material Included in Rough-Cut Planning
      "UEETO"                   , # Overdelivery tolerance limit
#      "UEETK"                   , # Indicator: Unlimited Overdelivery Allowed
      "UNETO"                   , # Underdelivery tolerance limit
      "WZEIT"                   , # Total replenishment lead time (in workdays)
#      "ATPKZ"                   , # Replacement part
#      "VZUSL"                   , # Surcharge factor for cost in percent
#      "HERBL"                   , # State of manufacture
      "INSMK"                   , # Post to Inspection Stock
#      "SPROZ"                   , # Sample for quality inspection (in %) (deactivated)
#      "QUAZT"                   , # Quarantine period (deactivated)
#      "SSQSS"                   , # Control Key for Quality Management in Procurement
#      "MPDAU"                   , # Mean inspection duration (deactivated)
#      "KZPPV"                   , # Indicator for inspection plan (deactivated)
      "KZDKZ"                   , # Documentation required indicator
#      "WSTGH"                   , # Active substance content (deactivated)
      "PRFRQ"                   , # Interval Until Next Recurring Inspection
#      "NKMPR"                   , # Date according to check sampling inspection (deactivated)
      "UMLMC"                   , # Stock in transfer (plant to plant)
      "LADGR"                   , # Loading Group
      "XCHPF"                   , # Batch Management Requirement Indicator for Plant
#      "USEQU"                   , # Quota arrangement usage
      "LGRAD"                   , # Service level
#      "AUFTL"                   , # Splitting Indicator
#      "PLVAR"                   , # Plan Version
#      "OTYPE"                   , # Object Type
#      "OBJID"                   , # Object ID
      "MTVFP"                   , # Checking Group for Availability Check
#      "PERIV"                   , # Fiscal Year Variant
      "KZKFK"                   , # Indicator: take correction factors into account
#      "VRVEZ"                   , # Shipping setup time
#      "VBAMG"                   , # Base quantity for capacity planning in shipping
#      "VBEAZ"                   , # Shipping processing time
#      "LIZYK"                   , # Deactivated
#      "BWSCL"                   , # Source of Supply
      "KAUTB"                   , # Indicator: "automatic purchase order allowed"
      "KORDB"                   , # Indicator: Source list requirement
      "STAWN"                   , # Commodity Code
      "HERKL"                   , # Country of Origin of Material (Non-Preferential Origin)
      "HERKR"                   , # Region of Origin of Material (Non-Preferential Origin)
      "EXPME"                   , # Unit of measure for commodity code (foreign trade)
#      "MTVER"                   , # Material Group for Intrastat
      "PRCTR"                   , # Profit Center
      "TRAME"                   , # Stock in Transit
      "MRPPP"                   , # PPC Planning Calendar
#      "SAUFT"                   , # Indicator: Repetitive Manufacturing Allowed
      "FXHOR"                   , # Planning time fence
      "VRMOD"                   , # Consumption mode
      "VINT1"                   , # Consumption period: backward
      "VINT2"                   , # Consumption period: forward
      "VERKZ"                   , # Version Indicator
      "STLAL"                   , # Alternative BOM
      "STLAN"                   , # BOM Usage
#      "PLNNR"                   , # Key for Task List Group
#      "APLAL"                   , # Group Counter
      "LOSGR"                   , # Lot Size for Product Costing
      "SOBSK"                   , # Special Procurement Type for Costing
      "FRTME"                   , # Production unit
      "LGPRO"                   , # Issue Storage Location
      "DISGR"                   , # MRP Group
      "KAUSF"                   , # Component Scrap in Percent
#      "QZGTP"                   , # Certificate Type
      "QMATV"                   , # Inspection Setup Exists for Material/Plant
      "TAKZT"                   , # Takt time
      "RWPRO"                   , # Range of coverage profile
#      "COPAM"                   , # Local field name for CO/PA link to SOP
#      "ABCIN"                   , # Physical Inventory Indicator for Cycle Counting
      "AWSLS"                   , # Variance Key
      "SERNP"                   , # Serial Number Profile
#      "CUOBJ"                   , # Internal object number
#      "STDPD"                   , # Configurable material
#      "SFEPR"                   , # Repetitive Manufacturing Profile
#      "XMCNG"                   , # Negative stocks allowed in plant
#      "QSSYS"                   , # Required QM System for Supplier
      "LFRHY"                   , # Planning cycle
#      "RDPRF"                   , # Rounding Profile
#      "VRBMT"                   , # Reference material for consumption
#      "VRBWK"                   , # Reference plant for consumption
#      "VRBDT"                   , # To date of the material to be copied for consumption
#      "VRBFK"                   , # Multiplier for reference material for consumption
      "AUTRU"                   , # Reset Forecast Model Automatically
#      "PREFE"                   , # Customs Preference
#      "PRENC"                   , # Exemption certificate: Indicator for legal control
#      "PRENO"                   , # Exemption certificate number for legal control
#      "PREND"                   , # Exemption certificate: Issue date of exemption certificate
#      "PRENE"                   , # Indicator: Vendor declaration exists
      "PRENG"                   , # Validity date of vendor declaration
#      "ITARK"                   , # Indicator: Military goods
#      "SERVG"                   , # IS-R service level
#      "KZKUP"                   , # Indicator: Material can be co-product
      "STRGR"                   , # Planning Strategy Group
#      "CUOBV"                   , # Internal object number of configurable material for planning
      "LGFSB"                   , # Default storage location for external procurement
      "SCHGT"                   , # Indicator: bulk material
#      "CCFIX"                   , # CC indicator is fixed
      "EPRIO"                   , # Stock determination group
#      "QMATA"                   , # Material Authorization Group for Activities in QM
#      "RESVP"                   , # Period of adjustment for planned independent requirements
#      "PLNTY"                   , # Task List Type
#      "UOMGR"                   , # Unit of Mearsure Group (Oil, Natural Gas,...)
#      "UMRSL"                   , # Conversion Group (Oil, Natural Gas,..)
#      "ABFAC"                   , # Air Bouyancy Factor
      "SFCPF"                   , # Production Scheduling Profile
      "SHFLG"                   , # Safety time indicator (with or without safety time)
      "SHZET"                   , # Safety time (in workdays)
#     "MDACH"                   , # Action Control: Planned Order Processing
      "KZECH"                   , # Determination of batch entry in the production/process order
#      "MEGRU"                   , # Unit of Measure Group
#      "MFRGR"                   , # Material freight group
#      "PROFIL"                  , # Name of Backflush Profile
#      "VKUMC"                   , # Stock transfer sales value (plant to plant) for VO material
#      "VKTRW"                   , # Transit value at sales price for value-only material
#      "KZAGL"                   , # Indicator: smooth promotion consumption
      "FVIDK"                   , # Production Version To Be Costed
#      "FXPRU"                   , # Fixed-Price Co-Product
      "LOGGR"                   , # Logistics handling group for workload calculation
#      "FPRFM"                   , # Distribution profile of material in plant
#      "GLGMG"                   , # Tied Empties Stock
#      "VKGLG"                   , # Sales value of tied empties stock
#      "INDUS"                   , # Material CFOP category
#      "MOWNR"                   , # CAP: Number of CAP products list
#      "MOGRU"                   , # Common Agricultural Policy: CAP products group-Foreign Trade
#      "CASNR"                   , # CAS number for pharmaceutical products in foreign trade
      "GPNUM"                   , # Production statistics: PRODCOM number for foreign trade
#      "STEUC"                   , # Control code for consumption taxes in foreign trade
#      "FABKZ"                   , # Indicator: Item Relevant to JIT Delivery Schedules
      "MATGR"                   , # Group of Materials for Transition Matrix
#      "VSPVB"                   , # Proposed Supply pa_AREA in Material Master Record
#      "DPLFS"                   , # Fair share rule
#      "DPLPU"                   , # Indicator: push distribution
#      "DPLHO"                   , # Deployment horizon in days
#      "MINLS"                   , # Minimum lot size for Supply Demand Match
#      "MAXLS"                   , # Maximum lot size for Supply Demand Match
#      "FIXLS"                   , # Fixed lot size for Supply Demand Match
#      "LTINC"                   , # Lot size increment for  Supply Demand Match
#      "COMPL"                   , # This field is no longer used
#      "CONVT"                   , # Conversion types for production figures
      "SHPRO"                   , # Period Profile for Safety Time
#      "AHDIS"                   , # MRP relevancy for dependent requirements
      "DIBER"                   , # Indicator: MRP pa_AREA exists
#      "KZPSP"                   , # Indicator for cross-project material
#      "OCMPF"                   , # Overall profile for order change management
#      "APOKZ"                   , # Indicator: Is material relevant for APO
#      "MCRUE"                   , # MARDH rec. already exists for per. before last of MARD per.
      "LFMON"                   , # Current period (posting period)
      "LFGJA"                   , # Fiscal Year of Current Period
      "EISLO"                   , # Minimum Safety Stock
      "NCOST"                   , # Do Not Cost
#      "ROTATION_DATE"           , # Strategy for Putaway and Stock Removal
#      "UCHKZ"                   , # Indicator for Original Batch Management
#      "UCMAT"                   , # Reference Material for Original Batches
#      "EXCISE_TAX_RLVNCE"       , # Excise Tax Relevance Indicator
      "BWESB"                   , # Valuated Goods Receipt Blocked Stock
#      "SGT_COVS"                , # Segmentation Strategy
#      "SGT_STATC"               , # Segmentation Status
#      "SGT_SCOPE"               , # Segmentation Strategy Scope
#      "SGT_MRPSI"               , # Sort Stock based on Segment
#      "SGT_PRCM"                , # Consumption Priority [Obsolete]
#      "SGT_CHINT"               , # Discrete Batch Number [Obsolete]
#      "SGT_STK_PRT"             , # Stock Protection Indicator [Obsolete]
#      "SGT_DEFSC"               , # Default Stock Segment value
#      "SGT_MRP_ATP_STATUS"      , # ATP/MRP Status for Material and Segment [Obsolete]
#      "SGT_MMSTD"               , # Date from which the plant-spcific mat. status is valid [OBS]
#      "FSH_MG_ARUN_REQ"         , # Supply Assignment (ARun)
#      "FSH_SEAIM"               , # Indicator: Season Active in Inventory Management
#      "FSH_VAR_GROUP"           , # Variant Group
#      "FSH_KZECH"               , # Indicator: Batch Assignt. during TR to TO conv [OBSOLETE]
#      "FSH_CALENDAR_GROUP"      , # Calendar Group
#      "ARUN_FIX_BATCH"          , # Assign Batches in Supply Assignment (ARun)
#      "PPSKZ"                   , # Indicator for Advanced Planning
#      "CONS_PROCG"              , # Consignment Control
#      "GI_PR_TIME"              , # Goods Issue Processing Time in Days
#      "MULTIPLE_EKGRP"          , # Purchasing Across Purchasing Group
#      "REF_SCHEMA"              , # Reference Determination Schema
#      "MIN_TROC"                , # Minimum Target Range of Coverage
#      "MAX_TROC"                , # Maximum Target Range of Coverage
#      "TARGET_STOCK"            , # Target Stock
#      "NF_FLAG"                 , # Indicator: Material Contains NF Metals
#      "/CWM/UMLMC"              , # Stock in transfer (plant to plant)
#      "/CWM/TRAME"              , # Stock in Transit
#      "/CWM/BWESB"              , # Valuated Goods Receipt Blocked Stock
      "SCM_MATLOCID_GUID16"     , # Internal Key for Product
      "SCM_MATLOCID_GUID22"     , # Internal Number (UID) for Location Product
      "SCM_GRPRT"               , # Goods Receipt Processing Time
      "SCM_GIPRT"               , # Goods Issue Processing Time
#      "SCM_SCOST"               , # Product-Dependent Storage Costs
#      "SCM_RELDT"               , # Replenishment Lead Time in Calendar Days
#      "SCM_RRP_TYPE"            , # PP Planning Procedure
#      "SCM_HEUR_ID"             , # PPC Heuristics
#      "SCM_PACKAGE_ID"          , # Planning Package to Which Product Belongs
#      "SCM_SSPEN"               , # Penalty Costs for Safety Stock Violation
#      "SCM_GET_ALERTS"          , # Alert Relevance of Product
#      "SCM_RES_NET_NAME"        , # Resource Network
#      "SCM_CONHAP"              , # Handling Capacity Consumption in Unit of Measure (Gds Rcpt)
#      "SCM_HUNIT"               , # Unit of Measure: Handling Capacity in Goods Receipt
#      "SCM_CONHAP_OUT"          , # Handling Capacity Consumption in Unit of Measure (Gds Issue)
#      "SCM_HUNIT_OUT"           , # Unit of Measure: Handling Capacity in Goods Issue
#      "SCM_SHELF_LIFE_LOC"      , # Use Location-Dependent Shelf Life of Product when Planning
#      "SCM_SHELF_LIFE_DUR"      , # Location-Dependent Shelf Life
#      "SCM_MATURITY_DUR"        , # Location-Dependent Maturation Time
#      "SCM_SHLF_LFE_REQ_MIN"    , # Minimum Shelf Life Required: Location-Dependent
#      "SCM_SHLF_LFE_REQ_MAX"    , # Maximum Shelf Life Required: Location-Dependent
#      "SCM_LSUOM"               , # Unit of Measure of Lot Size
#      "SCM_REORD_DUR"           , # Reorder Days' Supply (in Workdays)
#      "SCM_TARGET_DUR"          , # Target Days' Supply in Workdays
#      "SCM_TSTRID"              , # Planning Calendar for Periodic Lot Sizing Procedure
#      "SCM_STRA1"               , # Requirement Strategy
#      "SCM_PEG_PAST_ALERT"      , # Alert threshold for delayed receipts
#      "SCM_PEG_FUTURE_ALERT"    , # Alert threshold for early receipts
#      "SCM_PEG_STRATEGY"        , # Pegging strategy for dynamic pegging
#      "SCM_PEG_WO_ALERT_FST"    , # Avoid Alerts in Pegging
#      "SCM_FIXPEG_PROD_SET"     , # Retain Fixed Pegging for Product on Document Change
#      "SCM_WHATBOM"             , # Plan Explosion
#      "SCM_RRP_SEL_GROUP"       , # Planning Group
#      "SCM_INTSRC_PROF"         , # Profile for Interactive Sourcing
#      "SCM_PRIO"                , # Priority of Product
#      "SCM_MIN_PASS_AMOUNT"     , # Minimum Passing Amount for Continuous I/O Pegging
#      "SCM_PROFID"              , # Conversion Rule
#      "SCM_GES_MNG_USE"         , # Use/Consume Entire Quantity of a Receipt Element
#      "SCM_GES_BST_USE"         , # Use/Consume Entire Quantity of a Stock Element
#      "ESPPFLG"                 , # Usage in Extended Service Parts Planning
#      "SCM_THRUPUT_TIME"        , # Throughput Time
#      "SCM_TPOP"                , # Third-Party Order Processing
#      "SCM_SAFTY_V"             , # Safety Stock for Virtual Child Location
#      "SCM_PPSAFTYSTK"          , # Safety Stock at Parent Location
#      "SCM_PPSAFTYSTK_V"        , # Safety Stock of Parent Location Virtual Child Location
#      "SCM_REPSAFTY"            , # Repair Safety Stock
#      "SCM_REPSAFTY_V"          , # Repair Safety Stock for Virtual Child Location
#      "SCM_REORD_V"             , # Reorder Point for Virtual Child Location
#      "SCM_MAXSTOCK_V"          , # Maximum Stock Level for Virtual Child Locations
#      "SCM_SCOST_PRCNT"         , # Cost Factor for Stockholding Costs
#      "SCM_PROC_COST"           , # Procurement Costs for Product
#      "SCM_NDCOSTWE"            , # Goods Receiving Costs
#      "SCM_NDCOSTWA"            , # Goods Issue Costs
#      "SCM_CONINP"              , # Consumption of Storage Capacity per Unit of Material
#      "CONF_GMSYNC"             , # Synchronous Posting of Goods Issue
#      "SCM_IUNIT"               , # Unit of Measurement of Size
#      "SCM_SFT_LOCK"            , # Indicator for Safety Stock Override
#      "DUMMY_PLNT_INCL_EEW_PS"  , # MD product plant extensible field element
#      "/SAPMP/TOLPRPL"          , # Percentage Tolerance Plus
#      "/SAPMP/TOLPRMI"          , # Percentage Tolerance Minus
#      "/STTPEC/SERVALID"        , # Serialization Valid From
#      "/VSO/R_PKGRP"            , # Packing Group of the Material (VSO)
#      "/VSO/R_LANE_NUM"         , # Line within the Automatic Picking Zone (VSO)
#      "/VSO/R_PAL_VEND"         , # Material No. of the Packaging Material of the Vendor (VSO)
#      "/VSO/R_FORK_DIR"         , # Pick Packaging Materials only Lengthwise (VSO)
#      "IUID_RELEVANT"           , # IUID-Relevant
#      "IUID_TYPE"               , # Structure Type of UII
#      "UID_IEA"                 , # External Allocation of UII
#      "DPCBT"                   , # Single-Unit Batch Def. Value
      "VARWI"                   , # Variable Weight enabled
      "TOLVW"                   , # Variable weight Tolerance
      "DVWCH"                     # Disable Variable Weight check
    )
    
  }

fMARC_REL1 <- 
  fMARC_ALL <-
  function(){
    list(
      "MATNR"                   , # Material Number                                               X
      "WERKS"                   , # Plant                                                         X
      "PSTAT"                   , # Maintenance status
      "LVORM"                   , # Flag Material for Deletion at Plant Level
      "BWTTY"                   , # Valuation Category
      "XCHAR"                   , # Batch management indicator (internal)
      "MMSTA"                   , # Plant-Specific Material Status
      "MMSTD"                   , # Date from Which the Plant-Specific Material Status Is Valid
      "MAABC"                   , # ABC Indicator
      "EKGRP"                   , # Purchasing Group
      "AUSME"                   , # Unit of issue
      "DISMM"                   , # MRP Type
      "DISPO"                   , # MRP Controller
      "PLIFZ"                   , # Planned Delivery Time in Days
      "WEBAZ"                   , # Goods receipt processing time in days
      "PERKZ"                   , # Period Indicator
      "DISLS"                   , # Lot Sizing Procedure within Materials Planning
      "BESKZ"                   , # Procurement Type
      "SOBSL"                   , # Special procurement type
      "EISBE"                   , # Safety Stock
      "BSTMI"                   , # Minimum Lot Size
      "BSTMA"                   , # Maximum Lot Size
      "BSTFE"                   , # Fixed lot size
      "BSTRF"                   , # Rounding value for purchase order quantity
      "SBDKZ"                   , # Dependent requirements ind. for individual and coll. reqmts
      "LAGPR"                   , # Storage Costs Percentage Code
      "KZAUS"                   , # Discontinuation indicator
      "AUSDT"                   , # Effective-Out Date
      "NFMAT"                   , # Follow-Up Material
      "RGEKZ"                   , # Indicator: Backflush
      "FEVOR"                   , # Production Supervisor
      "DZEIT"                   , # In-house production time
      "MAXLZ"                   , # Maximum Storage Period
      "LZEIH"                   , # Unit for maximum storage period
      "WZEIT"                   , # Total replenishment lead time (in workdays)
      "XCHPF"                   , # Batch Management Requirement Indicator for Plant
      "LGRAD"                   , # Service level
      "MTVFP"                   , # Checking Group for Availability Check
      "KZKFK"                   , # Indicator: take correction factors into account
      "KAUTB"                   , # Indicator: "automatic purchase order allowed"
      "KORDB"                   , # Indicator: Source list requirement !!!!!!!!!!!!!!!!!!!!!!
      "FXHOR"                   , # Planning time fence
      "VRMOD"                   , # Consumption mode
      "VINT1"                   , # Consumption period: backward
      "VINT2"                   , # Consumption period: forward
      "VERKZ"                   , # Version Indicator
      "STLAL"                   , # Alternative BOM
      "STLAN"                   , # BOM Usage
      "LOSGR"                   , # Lot Size for Product Costing
      "SOBSK"                   , # Special Procurement Type for Costing
      "FRTME"                   , # Production unit
      "LGPRO"                   , # Issue Storage Location
      "DISGR"                   , # MRP Group
      "KAUSF"                   , # Component Scrap in Percent
      "TAKZT"                   , # Takt time
      "RWPRO"                   , # Range of coverage profile
      "LFRHY"                   , # Planning cycle
      "AUTRU"                   , # Reset Forecast Model Automatically
      "STRGR"                   , # Planning Strategy Group
      "LGFSB"                   , # Default storage location for external procurement
      "SCHGT"                   , # Indicator: bulk material
      "EPRIO"                   , # Stock determination group
      "SFCPF"                   , # Production Scheduling Profile
      "SHFLG"                   , # Safety time indicator (with or without safety time)
      "SHZET"                   , # Safety time (in workdays)
      "KZECH"                   , # Determination of batch entry in the production/process order
      "MATGR"                   , # Group of Materials for Transition Matrix
      "SHPRO"                   , # Period Profile for Safety Time
      "DIBER"                   , # Indicator: MRP pa_AREA exists
      "EISLO"                   , # Minimum Safety Stock
      "NCOST"                   , # Do Not Cost
      "BWESB"                   , # Valuated Goods Receipt Blocked Stock
      "SCM_GRPRT"               , # Goods Receipt Processing Time
      "SCM_GIPRT"               #, # Goods Issue Processing Time

    )
    
  }
