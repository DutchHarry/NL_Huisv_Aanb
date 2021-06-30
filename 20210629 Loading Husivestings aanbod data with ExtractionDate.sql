/*

CREATE a database HUISV_AANBOD
*/


USE HUISV_AANBOD;
GO

DROP TABLE IF EXISTS [tbl_huisvesting_aanbod_data_allE];
GO

CREATE TABLE [tbl_huisvesting_aanbod_data_allE](
	[hID] [bigint] IDENTITY(1,1) NOT NULL,
	[StringLength]  AS (len(isnull([data],''))),
	[StringHash]  AS (CONVERT([varchar](32),hashbytes('md5',isnull([data],'')),(2))),
  [ExtractionFileDate] datetime NULL,
  [ExtractionFileName] varchar(255) NULL,
  [ExtractionDate] datetime NULL,
	[site] [varchar](255) NULL,
	[wid] [varchar](20) NULL,
	[data] [nvarchar](max) NULL
);
GO

ALTER TABLE [tbl_huisvesting_aanbod_data_allE] REBUILD PARTITION = ALL  
WITH (DATA_COMPRESSION = ROW);   
GO  
ALTER TABLE [tbl_huisvesting_aanbod_data_allE] REBUILD PARTITION = ALL
    WITH (DATA_COMPRESSION = PAGE);
GO

--create view for import
CREATE OR ALTER VIEW [Vw_huisvesting_aanbod_data_allE] AS 
SELECT
  [ExtractionFileDate]
, [ExtractionFileName]
, [site]
, [ExtractionDate]
, [wid]
, [data]
FROM [tbl_huisvesting_aanbod_data_allE];
GO


-- import the JSON files
DECLARE @ExtractionFileName varchar(255) = ''--'S:\_ue\WOZ\VBO1_DATA_1.json';
DECLARE @ExtractionFileDate varchar(255) = ''
DECLARE @filename varchar(255) = '';
DECLARE @filedir varchar(255) = 'D:\NLDATA\JSON_huisv_aanb\with extraction date\';  --<-- CHANGE!!!!
DECLARE @tablename varchar(1000) = 'Vw_huisvesting_aanbod_data_allE';
DECLARE @featurecount int = 0; --for itterating features
DECLARE @featurestring varchar(5) = CONVERT(VARCHAR(5),@featurecount);
DECLARE @maxfeatures int = 372; 
DECLARE @minfeatures int = 0; 
DECLARE @sql nvarchar(max) ='';
DECLARE @debug varchar(5) = 'Y'; --Y,I,N
DECLARE @quote varchar(5)= '''';
DECLARE @crlf varchar(5) = CHAR(13)+CHAR(10);
DECLARE @msg varchar(8000) = ''
DECLARE @doscommand varchar(8000);
DECLARE @result int; 


-- BEGIN get dir into table
DROP TABLE IF EXISTS #CommandShell;
CREATE TABLE #CommandShell ( Line VARCHAR(512));
SET @doscommand = 'dir "'+@filedir+ '" /TC';
PRINT @doscommand;
INSERT INTO #CommandShell
EXEC @result = MASTER..xp_cmdshell   @doscommand ;
IF (@result = 0)  
   PRINT 'Success getting filelist from '+@filedir  
ELSE  
   PRINT 'Failure getting filelist from '+@filedir  
;
DELETE   --irrelevant lines
FROM   #CommandShell
WHERE  Line NOT LIKE '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] %'
OR Line LIKE '%<DIR>%'
OR Line is null
;
-- END get dir into table


--TRUNCATE TABLE [tbl_huisvesting_aanbod_data_allE];

DECLARE wiz_json_file CURSOR 
  FAST_FORWARD
  FOR 
  SELECT
    ExtractionFileDate = FORMAT(CONVERT(DATETIME2,LEFT(Line,17)+':00',103), 'dd-MMM-yyyy HH:mm:ss')
  , ExtractionFileName = REVERSE( LEFT(REVERSE(Line),CHARINDEX(' ',REVERSE(line))-1 ) )
  FROM #CommandShell
  ORDER BY 
    ExtractionFileName

OPEN wiz_json_file  
FETCH NEXT FROM wiz_json_file
INTO @ExtractionFileDate, @ExtractionFileName;
WHILE @@FETCH_STATUS = 0  
BEGIN  
  PRINT @filename;
  --BEGIN DO SOMETHING WITHIN CURSOR
	--BEGIN JSON FILE PROCESSING

SET @sql = '
INSERT INTO ['+@tablename+']
SELECT 
  '+@quote+@ExtractionFileDate+@quote+'
, '+@quote+@ExtractionFileName+@quote+'
, SUBSTRING('+@quote+@ExtractionFileName+@quote+',6,CHARINDEX(''_id_'','+@quote+@ExtractionFileName+@quote+')-6)
, wiz.*
FROM OPENROWSET (BULK '+@quote+@filedir+@ExtractionFileName+@quote+', SINGLE_CLOB) as j
CROSS APPLY OPENJSON(BulkColumn)
WITH( 
  [Extractiondate] datetime '+@quote+'$.ExtractionDate'+@quote+'
, [wid] varchar(8000) '+@quote+'$.wid'+@quote+'
, [data] nvarchar(max) AS JSON
) AS wiz
WHERE 1=1
;
'
		IF @debug = 'Y' 
			BEGIN
			IF LEN(@sql) > 2047
				PRINT @sql;
			ELSE
				RAISERROR (@sql, 0, 1) WITH NOWAIT; 
			END;
		EXEC (@sql);

	--END JSON FILE PROCESSING
  --END   DO SOMETHING WITHIN CURSOR
  FETCH NEXT FROM wiz_json_file   
    INTO @ExtractionFileDate, @ExtractionFileName;
END   
CLOSE wiz_json_file;  
DEALLOCATE wiz_json_file;  


-- drop directory table
DROP TABLE IF EXISTS #CommandShell;
PRINT 'ALL HUISV_AANBOD JSON LOADED';
GO


--clean a few sites from downloadingnames
--if you decide to speed up download a bit by splitting in diffeent processes
UPDATE [tbl_huisvesting_aanbod_data_allE]
   SET [site] = LEFT([site],LEN([site])-1)
WHERE 1=1
AND [site] IN ('woonnet-haaglanden1','woonnet-haaglanden2','klikvoorwonenZ','hurennoordveluweZ')
GO

/*
SELECT COUNT(*)
FROM [tbl_huisvesting_aanbod_data_allE]
*/


-- delete fulL duplicates (identical hash; keep newest date
;WITH cte AS (
SELECT 
  ROW_NUMBER() OVER (
    PARTITION BY 
    [StringHash]
	, [site]
	, [wid]
    ORDER BY 
    [StringHash]
	, [site]
	, [wid]
	, [ExtractionDate] desc
  ) AS row_num
FROM [tbl_huisvesting_aanbod_data_allE]
)
DELETE FROM cte
WHERE 
1=1
AND row_num > 1
;


--delete duplicate id with different hash, keep newest
;WITH cte AS (
SELECT 
  ROW_NUMBER() OVER (
    PARTITION BY 
	  [site]
	, [wid]
    ORDER BY 
	  [site]
	, [wid]
	, [ExtractionDate] desc
	, [StringLength] desc
  ) AS row_num
FROM [tbl_huisvesting_aanbod_data_allE]
)
DELETE FROM cte
WHERE 
1=1
AND row_num > 1
;


--dissect data

--select from json wiz columns
--very similar code in view [Vw_huisvesting_aanbod_data_allE_dissected]

DROP TABLE IF EXISTS [tbl_huisvesting_aanbod_data_allE_dissected];
GO

CREATE OR ALTER VIEW Vw_huisvesting_aanbod_data_allE_dissected AS
WITH cte0 AS (
SELECT --top 10
  w.[hID]
, w.[StringLength]
, w.[StringHash]
, w.[wid]
, w.[ExtractionDate]
, w.[site]
, w.[data]
, wiz1.*
FROM [tbl_huisvesting_aanbod_data_allE] w
CROSS APPLY OPENJSON(w.[data]) 
WITH (
  infoveldBewoners VARCHAR(MAX) '$.infoveldBewoners',
  infoveldKort VARCHAR(MAX) '$.infoveldKort',
  hospiterenVanaf VARCHAR(MAX) '$.hospiterenVanaf',
  extraInformatieUrl VARCHAR(MAX) '$.extraInformatieUrl',
  huurtoeslagVoorwaarde nvarchar(max) AS JSON,
  huurtoeslagVoorwaarde_icon VARCHAR(MAX) '$.huurtoeslagVoorwaarde.icon',
  huurtoeslagVoorwaarde_localizedNaam VARCHAR(MAX) '$.huurtoeslagVoorwaarde.localizedNaam',
  huurtoeslagVoorwaarde_localizedIconText VARCHAR(MAX) '$.huurtoeslagVoorwaarde.localizedIconText',
  sorteergroep nvarchar(max) AS JSON,
  sorteergroep_code VARCHAR(MAX) '$.sorteergroep.code',
  sorteergroep_id VARCHAR(MAX) '$.sorteergroep.id',
  huurtoeslagMogelijk VARCHAR(MAX) '$.huurtoeslagMogelijk',
  beschikbaarTot VARCHAR(MAX) '$.beschikbaarTot',
  actionLabelToelichting VARCHAR(MAX) '$.actionLabelToelichting',
  huurinkomenstabelGebruiken VARCHAR(MAX) '$.huurinkomenstabelGebruiken',
  voorrangUrgentie VARCHAR(MAX) '$.voorrangUrgentie',
  voorrangOverigeUrgenties VARCHAR(MAX) '$.voorrangOverigeUrgenties',
  voorrangHuishoudgrootteMin VARCHAR(MAX) '$.voorrangHuishoudgrootteMin',
  voorrangHuishoudgrootteMax VARCHAR(MAX) '$.voorrangHuishoudgrootteMax',
  voorrangLeeftijdMin VARCHAR(MAX) '$.voorrangLeeftijdMin',
  voorrangLeeftijdMax VARCHAR(MAX) '$.voorrangLeeftijdMax',
  voorrangGezinnenKinderen VARCHAR(MAX) '$.voorrangGezinnenKinderen',
  voorrangKernbinding VARCHAR(MAX) '$.voorrangKernbinding',
  woningVoorrangVoor nvarchar(max) AS JSON,
  woningVoorrangVoor_localizedName VARCHAR(MAX) '$.woningVoorrangVoor.localizedName',
  specifiekeVoorzieningen nvarchar(max) AS JSON,
--  specifiekeVoorzieningen_description VARCHAR(MAX) '$.specifiekeVoorzieningen.description',
  reactionData nvarchar(max) AS JSON,
  reactionData_mogelijkePositie VARCHAR(MAX) '$.reactionData.mogelijkePositie',
  reactionData_voorlopigePositie VARCHAR(MAX) '$.reactionData.voorlopigePositie',
  reactionData_kanReageren VARCHAR(MAX) '$.reactionData.kanReageren',
  reactionData_isPassend VARCHAR(MAX) '$.reactionData.isPassend',
  reactionData_redenMagNietReagerenCode VARCHAR(MAX) '$.reactionData.redenMagNietReagerenCode',
  --[reactionData_winkel-reactie-nietmeergepubliceerd] VARCHAR(MAX) '$.reactionData.winkel-reactie-nietmeergepubliceerd',
  reactionData_loggedin VARCHAR(MAX) '$.reactionData.loggedin',
  reactionData_action VARCHAR(MAX) '$.reactionData.action',
  reactionData_objecttype VARCHAR(MAX) '$.reactionData.objecttype',
  reactionData_label VARCHAR(MAX) '$.reactionData.label',
  reactionData_openExterneLink VARCHAR(MAX) '$.reactionData.openExterneLink',
  reactionData_isVrijeSectorWoning VARCHAR(MAX) '$.reactionData.isVrijeSectorWoning',
  reactionData_url VARCHAR(MAX) '$.reactionData.url',
  isVrijeSectorWoning VARCHAR(MAX) '$.isVrijeSectorWoning',
  inschrijvingVereistVoorReageren VARCHAR(MAX) '$.inschrijvingVereistVoorReageren',
  corporation nvarchar(max) AS JSON,
  corporation_name VARCHAR(MAX) '$.corporation.name',
  corporation_picture nvarchar(max) '$.corporation.picture' AS JSON,
  corporation_picture_location VARCHAR(MAX) '$.corporation.picture.location',
  corporation_website VARCHAR(MAX) '$.corporation.website',
  postalcode VARCHAR(MAX) '$.postalcode',
  street VARCHAR(MAX) '$.street',
  houseNumber VARCHAR(MAX) '$.houseNumber',
  houseNumberAddition VARCHAR(MAX) '$.houseNumberAddition',
  regio nvarchar(max) AS JSON,
  regio_name VARCHAR(MAX) '$.regio.name',
  municipality nvarchar(max) AS JSON,
  municipality_name VARCHAR(MAX) '$.municipality.name',
  city nvarchar(max) AS JSON,
  city_name VARCHAR(MAX) '$.city.name',
  [quarter] nvarchar(max) AS JSON,
  quarter_name VARCHAR(MAX) '$.quarter.name',
  quarter_extraInformatieUrl VARCHAR(MAX) '$.quarter.extraInformatieUrl',
  quarter_id VARCHAR(MAX) '$.quarter.id',
  dwellingType nvarchar(max) AS JSON,
  dwellingType_categorie VARCHAR(MAX) '$.dwellingType.categorie',
  dwellingType_huurprijsDuurActief VARCHAR(MAX) '$.dwellingType.huurprijsDuurActief',
  dwellingType_localizedName VARCHAR(MAX) '$.dwellingType.localizedName',
  voorrangUrgentieReden nvarchar(max) AS JSON,
  voorrangUrgentieReden_localizedName VARCHAR(MAX) '$.voorrangUrgentieReden.localizedName',
  availableFrom VARCHAR(MAX) '$.availableFrom',
  netRent VARCHAR(MAX) '$.netRent',
  calculationRent VARCHAR(MAX) '$.calculationRent',
  totalRent VARCHAR(MAX) '$.totalRent',
  flexibelHurenActief VARCHAR(MAX) '$.flexibelHurenActief',
  heatingCosts VARCHAR(MAX) '$.heatingCosts',
  additionalCosts VARCHAR(MAX) '$.additionalCosts',
  serviceCosts VARCHAR(MAX) '$.serviceCosts',
  sellingPrice VARCHAR(MAX) '$.sellingPrice',
  description VARCHAR(MAX) '$.description',
  bestemming nvarchar(max) AS JSON,
  areaLivingRoom VARCHAR(MAX) '$.areaLivingRoom',
  areaSleepingRoom VARCHAR(MAX) '$.areaSleepingRoom',
  sleepingRoom nvarchar(max) AS JSON,
  sleepingRoom_amountOfRooms VARCHAR(MAX) '$.sleepingRoom.amountOfRooms',
  sleepingRoom_id VARCHAR(MAX) '$.sleepingRoom.id',
  sleepingRoom_localizedName VARCHAR(MAX) '$.sleepingRoom.localizedName',
  energyLabel nvarchar(max) AS JSON,
  energyLabel_icon VARCHAR(MAX) '$.energyLabel.icon',
  energyLabel_id VARCHAR(MAX) '$.energyLabel.id',
  energyLabel_localizedName VARCHAR(MAX) '$.energyLabel.localizedName',
  energyIndex VARCHAR(MAX) '$.energyIndex',
  [floor] nvarchar(max) AS JSON,
  floor_localizedName VARCHAR(MAX) '$.floor.localizedName',
  garden VARCHAR(MAX) '$.garden',
  gardenSite nvarchar(max) AS JSON,
  gardenSite_localizedName VARCHAR(MAX) '$.gardenSite.localizedName',
  oppervlakteTuin nvarchar(max) AS JSON,
  oppervlakteTuin_localizedName VARCHAR(MAX) '$.oppervlakteTuin.localizedName',
  balcony VARCHAR(MAX) '$.balcony',
  balconySite nvarchar(max) AS JSON,
  balconySite_localizedName VARCHAR(MAX) '$.balconySite.localizedName',
  heating nvarchar(max) AS JSON,
  heating_localizedName VARCHAR(MAX) '$.heating.localizedName',
  kitchen nvarchar(max) AS JSON,
  kitchen_localizedName VARCHAR(MAX) '$.kitchen.localizedName',
  attic nvarchar(max) AS JSON,
  attic_localizedName VARCHAR(MAX) '$.attic.localizedName',
  constructionYear VARCHAR(MAX) '$.constructionYear',
  minimumIncome VARCHAR(MAX) '$.minimumIncome',
  maximumIncome VARCHAR(MAX) '$.maximumIncome',
  minimumHouseholdSize VARCHAR(MAX) '$.minimumHouseholdSize',
  maximumHouseholdSize VARCHAR(MAX) '$.maximumHouseholdSize',
  minimumAge VARCHAR(MAX) '$.minimumAge',
  maximumAge VARCHAR(MAX) '$.maximumAge',
  inwonendeKinderenMinimum VARCHAR(MAX) '$.inwonendeKinderenMinimum',
  inwonendeKinderenMaximum VARCHAR(MAX) '$.inwonendeKinderenMaximum',
  [model] nvarchar(max) AS JSON,
  model_modelCategorie nvarchar(max) '$.model.modelCategorie'  AS JSON,
  model_modelCategorie_icon VARCHAR(MAX) '$.model.modelCategorie.icon',
  model_modelCategorie_code VARCHAR(MAX) '$.model.modelCategorie.code',
  model_modelCategorie_toonOpWebsite VARCHAR(MAX) '$.model.modelCategorie.toonOpWebsite',
  model_inCode VARCHAR(MAX) '$.model.inCode',
  model_isVoorExtraAanbod VARCHAR(MAX) '$.model.isVoorExtraAanbod',
  model_isHospiteren VARCHAR(MAX) '$.model.isHospiteren',
  model_advertentieSluitenNaEersteReactie VARCHAR(MAX) '$.model.advertentieSluitenNaEersteReactie',
  model_einddatumTonen VARCHAR(MAX) '$.model.einddatumTonen',
  model_aantalReactiesToenen VARCHAR(MAX) '$.model.aantalReactiesToenen',
  model_slaagkansTonen VARCHAR(MAX) '$.model.slaagkansTonen',
  model_id VARCHAR(MAX) '$.model.id',
  model_localizedName VARCHAR(MAX) '$.model.localizedName',
  rentBuy VARCHAR(MAX) '$.rentBuy',
  publicationDate VARCHAR(MAX) '$.publicationDate',
  closingDate VARCHAR(MAX) '$.closingDate',
  numberOfReactions VARCHAR(MAX) '$.numberOfReactions',
  assignmentID VARCHAR(MAX) '$.assignmentID',
  latitude VARCHAR(MAX) '$.latitude',
  longitude VARCHAR(MAX) '$.longitude',
  floorplans nvarchar(max) AS JSON,
  pictures nvarchar(max) AS JSON,
  gebruikFotoAlsHeader VARCHAR(MAX) '$.gebruikFotoAlsHeader',
  remainingTimeUntilClosingDate VARCHAR(MAX) '$.remainingTimeUntilClosingDate',
  reactieUrl VARCHAR(MAX) '$.reactieUrl',
  temporaryRent VARCHAR(MAX) '$.temporaryRent',
  showEnergyCosts VARCHAR(MAX) '$.showEnergyCosts',
  newlyBuild VARCHAR(MAX) '$.newlyBuild',
  storageRoom VARCHAR(MAX) '$.storageRoom',
  energyCosts nvarchar(max) AS JSON,
  areaDwelling VARCHAR(MAX) '$.areaDwelling',
  areaPerceel VARCHAR(MAX) '$.areaPerceel',
  volumeDwelling VARCHAR(MAX) '$.volumeDwelling',
  actionLabel nvarchar(max) AS JSON,
  actionLabel_localizedLabel VARCHAR(MAX) '$.actionLabel.localizedLabel',
  actionLabelFrom VARCHAR(MAX) '$.actionLabelFrom',
  actionLabelUntil VARCHAR(MAX) '$.actionLabelUntil',
  actionLabelIfActive VARCHAR(MAX) '$.actionLabelIfActive',
  relatieHuurInkomenData VARCHAR(MAX) '$.relatieHuurInkomenData',
  relatieHuurInkomenGroepen VARCHAR(MAX) '$.relatieHuurInkomenGroepen',
  doelgroepen nvarchar(max) AS JSON,
--  doelgroepen_icon VARCHAR(MAX) '$.doelgroepen.icon',
--  doelgroepen_code VARCHAR(MAX) '$.doelgroepen.code',
  koopvoorwaarden_localizedName VARCHAR(MAX) '$.koopvoorwaarden.localizedName',
  koopprijsType nvarchar(max) AS JSON,
  koopprijsType_localizedName VARCHAR(MAX) '$.koopprijsType.localizedName',
  koopkorting nvarchar(max) AS JSON,
  koopkorting_localizedName VARCHAR(MAX) '$.koopkorting.localizedName',
  koopproducten nvarchar(max) AS JSON,
  koopproducten_url VARCHAR(MAX) '$.koopproducten.url',
  koopproducten_picture VARCHAR(MAX) '$.koopproducten.picture',
  koopproducten_localizedName VARCHAR(MAX) '$.koopproducten.localizedName',
  isExtraAanbod VARCHAR(MAX) '$.isExtraAanbod',
  makelaars nvarchar(max) AS JSON,
  lengte VARCHAR(MAX) '$.lengte',
  breedte VARCHAR(MAX) '$.breedte',
  hoogte VARCHAR(MAX) '$.hoogte',
  rentDuration nvarchar(max) AS JSON,
  rentDuration_inCode VARCHAR(MAX) '$.rentDuration.inCode',
  rentDuration_id VARCHAR(MAX) '$.rentDuration.id',
  vatInclusive VARCHAR(MAX) '$.vatInclusive',
  isGepubliceerdInEenModelMetReageren VARCHAR(MAX) '$.isGepubliceerdInEenModelMetReageren',
  woningsoort nvarchar(max) AS JSON,
  woningsoort_isZelfstandig VARCHAR(MAX) '$.woningsoort.isZelfstandig',
  woningsoort_id VARCHAR(MAX) '$.woningsoort.id',
  woningsoort_localizedName VARCHAR(MAX) '$.woningsoort.localizedName',
  aantalMedebewoners VARCHAR(MAX) '$.aantalMedebewoners',
  isExternModelType VARCHAR(MAX) '$.isExternModelType',
  isZelfstandig VARCHAR(MAX) '$.isZelfstandig',
  urlKey VARCHAR(MAX) '$.urlKey',
  servicecomponentenBinnenServicekosten nvarchar(max) AS JSON,
  servicecomponentenBuitenServicekosten nvarchar(max) AS JSON,
  eenmaligeKosten VARCHAR(MAX) '$.eenmaligeKosten',
  reactieBeleidsregels nvarchar(max) AS JSON,
  sorteringBeleidsregels nvarchar(max) AS JSON,
  complex nvarchar(max) AS JSON,
  complex_nummer VARCHAR(MAX) '$.complex.nummer',
  complex_naam VARCHAR(MAX) '$.complex.naam',
  complex_url VARCHAR(MAX) '$.complex.url',
  complex_serviceovereenkomstVerplicht VARCHAR(MAX) '$.complex.serviceovereenkomstVerplicht',
  complex_serviceovereenkomstKosten VARCHAR(MAX) '$.complex.serviceovereenkomstVerplicht',
  complex_id VARCHAR(MAX) '$.complex.id',
  serviceovereenkomstKosten VARCHAR(MAX) '$.serviceovereenkomstKosten',
  extraInschrijfduurUitgeschakeld VARCHAR(MAX) '$.extraInschrijfduurUitgeschakeld',
  eigenaar nvarchar(max) AS JSON,
  eigenaar_name VARCHAR(MAX) '$.eigenaar.name',
  eigenaar_website VARCHAR(MAX) '$.eigenaar.website',
  eigenaar_logo VARCHAR(MAX) '$.eigenaar.logo',
  availableFromDate VARCHAR(MAX) '$.availableFromDate',
  zonnepanelen VARCHAR(MAX) '$.zonnepanelen',
  gaslozeWoning VARCHAR(MAX) '$.gaslozeWoning',
  nulOpDeMeterWoning VARCHAR(MAX) '$.nulOpDeMeterWoning',
  verzameladvertentieID VARCHAR(MAX) '$.verzameladvertentieID',
  ophalenInkomenViaMijnOverheidBijReagere VARCHAR(MAX) '$.ophalenInkomenViaMijnOverheidBijReagere',
  [id] VARCHAR(MAX) '$.id',
  isGepubliceerd VARCHAR(MAX) '$.isGepubliceerd',
  isInGepubliceerdeVerzameladvertentie VARCHAR(MAX) '$.isInGepubliceerdeVerzameladvertentie'
) AS wiz1
--CROSS APPLY OPENJSON(wiz1.[floorplans]) 
--WITH (
--  floorplan_uri VARCHAR(MAX) '$.uri'
--) AS wiz_floorplans
--CROSS APPLY OPENJSON(wiz1.pictures) 
--WITH (
--  picture_uri VARCHAR(MAX) '$.uri'
--) AS wiz_pictures
--CROSS APPLY OPENJSON(wiz1.specifiekeVoorzieningen) 
--WITH (
--  specifiekeVoorzieningen_description VARCHAR(MAX) '$.description',
--  specifiekeVoorzieningen_inCode VARCHAR(MAX) '$.inCode',
--  specifiekeVoorzieningen_dwellingTypeCategory VARCHAR(MAX) '$.dwellingTypeCategory',
--  specifiekeVoorzieningen_id VARCHAR(MAX) '$.id',
--  specifiekeVoorzieningen_localizedName VARCHAR(MAX) '$.localizedName'
--) AS wiz_specifiekeVoorzieningen
--CROSS APPLY OPENJSON(wiz1.doelgroepen) 
--WITH (
--  doelgroepen_code VARCHAR(MAX) '$.code'
--) AS wiz_doelgroepen
--CROSS APPLY OPENJSON(wiz1.makelaars) 
--WITH (
--  makelaars_qqq VARCHAR(MAX) '$.qqq'
--) AS wiz_makelaars
WHERE 1=1
--AND wiz1.postalcode LIKE '6006%'
--AND wiz1.street LIKE 'Serviliusstraat'
--AND wiz1.houseNumber LIKE '182'
--AND  w.[site] = 'thuisinlimburg'
--order by 
----  wiz1.availableFromDate
--  w.[site]
--, CONVERT(int,w.[wid])
)
, cte_floorplans AS (
SELECT
  w2.[hID]
, wiz_floorplans.floorplan_uri
FROM [tbl_huisvesting_aanbod_data_allE] w2
CROSS APPLY OPENJSON(w2.[data]) 
WITH (
  floorplans nvarchar(max) AS JSON
) wiz_data2
CROSS APPLY OPENJSON(wiz_data2.floorplans) 
WITH (
    floorplan_uri VARCHAR(MAX) '$.uri'
) AS wiz_floorplans
WHERE 1=1
)
, cte_pictures AS (
SELECT
  w3.[hID]
, wiz_pictures.picture_uri
FROM [tbl_huisvesting_aanbod_data_allE] w3
CROSS APPLY OPENJSON(w3.[data]) 
WITH (
  pictures nvarchar(max) AS JSON
) wiz_data3
CROSS APPLY OPENJSON(wiz_data3.pictures) 
WITH (
    picture_uri VARCHAR(MAX) '$.uri'
) AS wiz_pictures
WHERE 1=1
)
, cte_specifiekeVoorzieningen AS (
SELECT
  w4.[hID]
, wiz_specifiekeVoorzieningen.*
--, wiz_specifiekeVoorzieningen.specifiekeVoorzieningen_description
FROM [tbl_huisvesting_aanbod_data_allE] w4
CROSS APPLY OPENJSON(w4.[data]) 
WITH (
  specifiekeVoorzieningen nvarchar(max) AS JSON
) wiz_data4
CROSS APPLY OPENJSON(wiz_data4.specifiekeVoorzieningen) 
WITH (
  specifiekeVoorzieningen_description VARCHAR(MAX) '$.description',
  specifiekeVoorzieningen_inCode VARCHAR(MAX) '$.inCode',
  specifiekeVoorzieningen_dwellingTypeCategory VARCHAR(MAX) '$.dwellingTypeCategory',
  specifiekeVoorzieningen_id VARCHAR(MAX) '$.id',
  specifiekeVoorzieningen_localizedName VARCHAR(MAX) '$.localizedName'
) AS wiz_specifiekeVoorzieningen
WHERE 1=1
)
, cte_doelgroepen AS (
SELECT
  w5.[hID]
, wiz_doelgroepen.doelgroepen_code
FROM [tbl_huisvesting_aanbod_data_allE] w5
CROSS APPLY OPENJSON(w5.[data]) 
WITH (
  doelgroepen nvarchar(max) AS JSON
) wiz_data5
CROSS APPLY OPENJSON(wiz_data5.doelgroepen) 
WITH (
  doelgroepen_code VARCHAR(MAX) '$.code'
) AS wiz_doelgroepen
)
, cte_energyCosts AS (
SELECT
  w6.[hID]
, wiz_energyCosts.*
FROM [tbl_huisvesting_aanbod_data_allE] w6
CROSS APPLY OPENJSON(w6.[data]) 
WITH (
  energyCosts nvarchar(max) AS JSON
) wiz_data6
CROSS APPLY OPENJSON(wiz_data6.energyCosts) 
WITH (
  energyCosts_value VARCHAR(MAX) '$.value',
  energyCosts_energyProfile_icon VARCHAR(MAX) '$.energyProfile.icon'
) AS wiz_energyCosts
WHERE 1=1
)
, cte_reactieBeleidsregels AS(
SELECT
  w7.[hID]
, wiz_reactieBeleidsregels.*
FROM [tbl_huisvesting_aanbod_data_allE] w7
CROSS APPLY OPENJSON(w7.[data]) 
WITH (
  reactieBeleidsregels nvarchar(max) AS JSON
) wiz_data7
CROSS APPLY OPENJSON(wiz_data7.reactieBeleidsregels) 
WITH (
  reactieBeleidsregels_beleidsregel_code VARCHAR(MAX) '$.beleidsregel.code',
  reactieBeleidsregels_sortering VARCHAR(MAX) '$.sortering'
) AS wiz_reactieBeleidsregels
WHERE 1=1
)
, cte_final AS (
SELECT 
  cte0.*
, STUFF((
    SELECT '|' + floorplan_uri 
    FROM cte_floorplans As t1
    WHERE t1.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As Floorplans_string
, STUFF((
    SELECT '|' + picture_uri 
    FROM cte_pictures As t1
    WHERE t1.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As Pictures_string
, STUFF((
    SELECT '|' + doelgroepen_code 
    FROM cte_doelgroepen As t1
    WHERE t1.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As doelgroepen_string
, STUFF((
    SELECT '|' + specifiekeVoorzieningen_description 
    FROM cte_specifiekeVoorzieningen As t11
    WHERE t11.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As specifiekeVoorzieningen_description_string
, STUFF((
    SELECT '|' + specifiekeVoorzieningen_inCode 
    FROM cte_specifiekeVoorzieningen As t12
    WHERE t12.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As specifiekeVoorzieningen_inCode_string
, STUFF((
    SELECT '|' + specifiekeVoorzieningen_dwellingTypeCategory 
    FROM cte_specifiekeVoorzieningen As t13
    WHERE t13.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As specifiekeVoorzieningen_dwellingTypeCategory_string
, STUFF((
    SELECT '|' + specifiekeVoorzieningen_id 
    FROM cte_specifiekeVoorzieningen As t14
    WHERE t14.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As specifiekeVoorzieningen_id_string
, STUFF((
    SELECT '|' + specifiekeVoorzieningen_localizedName 
    FROM cte_specifiekeVoorzieningen As t15
    WHERE t15.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As specifiekeVoorzieningen_localizedName_string
, STUFF((
    SELECT '|' + energyCosts_value 
    FROM cte_energyCosts As t1
    WHERE t1.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As energyCosts_value_string
, STUFF((
    SELECT '|' + energyCosts_energyProfile_icon
    FROM cte_energyCosts As t1
    WHERE t1.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As energyCosts_energyProfile_icon_string
, STUFF((
    SELECT '|' + reactieBeleidsregels_beleidsregel_code
    FROM cte_reactieBeleidsregels As t1
    WHERE t1.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As reactieBeleidsregels_beleidsregel_code_string
, STUFF((
    SELECT '|' + reactieBeleidsregels_sortering
    FROM cte_reactieBeleidsregels As t1
    WHERE t1.hID = cte0.hID
    FOR XML PATH('')
  ), 1, 1, '') As reactieBeleidsregels_sortering_string
FROM cte0
)
SELECT TOP 1000000000
  * 
--INTO [tbl_huisvesting_aanbod_data_allE_dissected]
FROM cte_final
WHERE 1=1
--AND (site = 'thuisinlimburg' or site like '%accent%')
--AND reactieBeleidsregels <>'[]'
--and sorteringBeleidsregels <>'[]'
ORDER BY
  [site]
, CONVERT(int,[wid])
;
GO


CREATE OR ALTER       VIEW [dbo].[Vw_SitesInDatabase] aS
SELECT TOP 10000-- PERCENT
  [site]
, MIN(convert(int,[wid])) as MinOfWidOnline
, MAX(convert(int,[wid])) as MaxOfWidOnline
, COUNT(*) as TotalNumberOnline
FROM [HUISV_AANBOD].[dbo].[tbl_huisvesting_aanbod_data_allE]
WHERE 1=1
GROUP BY
  [site]
ORDER BY
  [site]
;
GO


CREATE OR ALTER     VIEW [dbo].[Vw_AdministratieKosten] AS
SELECT TOP 1000000000
  [hID]
--, availableFromDate
--, publicationDate	
--, closingDate
--, convert(DATETIME2,CASE WHEN availableFromDate ='' THEN left(publicationDate,10) ELSE availableFromDate END,23) AS [ProxyAdvertentieDatum]
, CASE WHEN CASE WHEN availableFromDate ='' THEN left(publicationDate,10) ELSE availableFromDate END > '2016-05-04' THEN 'Y' ELSE 'N' END AS [Laatste5jaar]
, CASE WHEN availableFromDate ='' THEN left(publicationDate,10) ELSE availableFromDate END AS [ProxyAdvertentieDatum]
, CASE WHEN ISNULL([eenmaligeKosten],'0') IN ('','0') THEN 'N' ELSE 'Y' END AS [AdministratiekostenGeadverteerd]
, [eenmaligeKosten]
, regio_name
, corporation_name
--,[StringLength]
--,[StringHash]
--,[wid]
--,[ExtractionDate]
--,[site]
--,[data]
--,[infoveldBewoners]
--,[infoveldKort]
--,[hospiterenVanaf]
--,[extraInformatieUrl]
--,[huurtoeslagVoorwaarde]
--,[huurtoeslagVoorwaarde_icon]
--,[huurtoeslagVoorwaarde_localizedNaam]
--,[huurtoeslagVoorwaarde_localizedIconText]
--,[sorteergroep]
--,[sorteergroep_code]
--,[sorteergroep_id]
--,[huurtoeslagMogelijk]
--,[beschikbaarTot]
--,[actionLabelToelichting]
--,[huurinkomenstabelGebruiken]
--,[voorrangUrgentie]
--,[voorrangOverigeUrgenties]
--,[voorrangHuishoudgrootteMin]
--,[voorrangHuishoudgrootteMax]
--,[voorrangLeeftijdMin]
--,[voorrangLeeftijdMax]
--,[voorrangGezinnenKinderen]
--,[voorrangKernbinding]
--,[woningVoorrangVoor]
--,[woningVoorrangVoor_localizedName]
--,[specifiekeVoorzieningen]
--,[reactionData]
--,[reactionData_mogelijkePositie]
--,[reactionData_voorlopigePositie]
--,[reactionData_kanReageren]
--,[reactionData_isPassend]
--,[reactionData_redenMagNietReagerenCode]
--,[reactionData_loggedin]
--,[reactionData_action]
--,[reactionData_objecttype]
--,[reactionData_label]
--,[reactionData_openExterneLink]
--,[reactionData_isVrijeSectorWoning]
--,[reactionData_url]
--,[isVrijeSectorWoning]
--,[inschrijvingVereistVoorReageren]
--,[corporation]
--,[corporation_name]
--,[corporation_picture]
--,[corporation_picture_location]
--,[corporation_website]
--,[postalcode]
--,[street]
--,[houseNumber]
--,[houseNumberAddition]
--,[regio]
--,[regio_name]
--,[municipality]
--,[municipality_name]
--,[city]
--,[city_name]
--,[quarter]
--,[quarter_name]
--,[quarter_extraInformatieUrl]
--,[quarter_id]
--,[dwellingType]
--,[dwellingType_categorie]
--,[dwellingType_huurprijsDuurActief]
--,[dwellingType_localizedName]
--,[voorrangUrgentieReden]
--,[voorrangUrgentieReden_localizedName]
--,[availableFrom]
--,[netRent]
--,[calculationRent]
--,[totalRent]
--,[flexibelHurenActief]
--,[heatingCosts]
--,[additionalCosts]
--,[serviceCosts]
--,[sellingPrice]
--,[description]
--,[bestemming]
--,[areaLivingRoom]
--,[areaSleepingRoom]
--,[sleepingRoom]
--,[sleepingRoom_amountOfRooms]
--,[sleepingRoom_id]
--,[sleepingRoom_localizedName]
--,[energyLabel]
--,[energyLabel_icon]
--,[energyLabel_id]
--,[energyLabel_localizedName]
--,[energyIndex]
--,[floor]
--,[floor_localizedName]
--,[garden]
--,[gardenSite]
--,[gardenSite_localizedName]
--,[oppervlakteTuin]
--,[oppervlakteTuin_localizedName]
--,[balcony]
--,[balconySite]
--,[balconySite_localizedName]
--,[heating]
--,[heating_localizedName]
--,[kitchen]
--,[kitchen_localizedName]
--,[attic]
--,[attic_localizedName]
--,[constructionYear]
--,[minimumIncome]
--,[maximumIncome]
--,[minimumHouseholdSize]
--,[maximumHouseholdSize]
--,[minimumAge]
--,[maximumAge]
--,[inwonendeKinderenMinimum]
--,[inwonendeKinderenMaximum]
--,[model]
--,[model_modelCategorie]
--,[model_modelCategorie_icon]
--,[model_modelCategorie_code]
--,[model_modelCategorie_toonOpWebsite]
--,[model_inCode]
--,[model_isVoorExtraAanbod]
--,[model_isHospiteren]
--,[model_advertentieSluitenNaEersteReactie]
--,[model_einddatumTonen]
--,[model_aantalReactiesToenen]
--,[model_slaagkansTonen]
--,[model_id]
--,[model_localizedName]
--,[rentBuy]
--,[publicationDate]
--,[closingDate]
--,[numberOfReactions]
--,[assignmentID]
--,[latitude]
--,[longitude]
--,[floorplans]
--,[pictures]
--,[gebruikFotoAlsHeader]
--,[remainingTimeUntilClosingDate]
--,[reactieUrl]
--,[temporaryRent]
--,[showEnergyCosts]
--,[newlyBuild]
--,[storageRoom]
--,[energyCosts]
--,[areaDwelling]
--,[areaPerceel]
--,[volumeDwelling]
--,[actionLabel]
--,[actionLabel_localizedLabel]
--,[actionLabelFrom]
--,[actionLabelUntil]
--,[actionLabelIfActive]
--,[relatieHuurInkomenData]
--,[relatieHuurInkomenGroepen]
--,[doelgroepen]
--,[koopvoorwaarden_localizedName]
--,[koopprijsType]
--,[koopprijsType_localizedName]
--,[koopkorting]
--,[koopkorting_localizedName]
--,[koopproducten]
--,[koopproducten_url]
--,[koopproducten_picture]
--,[koopproducten_localizedName]
--,[isExtraAanbod]
--,[makelaars]
--,[lengte]
--,[breedte]
--,[hoogte]
--,[rentDuration]
--,[rentDuration_inCode]
--,[rentDuration_id]
--,[vatInclusive]
--,[isGepubliceerdInEenModelMetReageren]
--,[woningsoort]
--,[woningsoort_isZelfstandig]
--,[woningsoort_id]
--,[woningsoort_localizedName]
--,[aantalMedebewoners]
--,[isExternModelType]
--,[isZelfstandig]
--,[urlKey]
--,[servicecomponentenBinnenServicekosten]
--,[servicecomponentenBuitenServicekosten]
--,[eenmaligeKosten]
--,[reactieBeleidsregels]
--,[sorteringBeleidsregels]
--,[complex]
--,[complex_nummer]
--,[complex_naam]
--,[complex_url]
--,[complex_serviceovereenkomstVerplicht]
--,[complex_serviceovereenkomstKosten]
--,[complex_id]
--,[serviceovereenkomstKosten]
--,[extraInschrijfduurUitgeschakeld]
--,[eigenaar]
--,[eigenaar_name]
--,[eigenaar_website]
--,[eigenaar_logo]
--,[availableFromDate]
--,[zonnepanelen]
--,[gaslozeWoning]
--,[nulOpDeMeterWoning]
--,[verzameladvertentieID]
--,[ophalenInkomenViaMijnOverheidBijReagere]
--,[id]
--,[isGepubliceerd]
--,[isInGepubliceerdeVerzameladvertentie]
--,[Floorplans_string]
--,[Pictures_string]
--,[doelgroepen_string]
--,[specifiekeVoorzieningen_description_string]
--,[specifiekeVoorzieningen_inCode_string]
--,[specifiekeVoorzieningen_dwellingTypeCategory_string]
--,[specifiekeVoorzieningen_id_string]
--,[specifiekeVoorzieningen_localizedName_string]
--,[energyCosts_value_string]
--,[energyCosts_energyProfile_icon_string]
--,[reactieBeleidsregels_beleidsregel_code_string]
--,[reactieBeleidsregels_sortering_string]
FROM [Vw_huisvesting_aanbod_data_allE_dissected]
WHERE 1=1
AND Convert(float,[eenmaligeKosten])>0
--AND corporation_name LIKE 'Wonen%Limburg%'
--AND corporation_name IN ('Wonen Limburg Accent','Wonen Limburg')
--AND isnull([eenmaligeKosten],'0') IN ('','0')
ORDER BY 
  Convert(float,[eenmaligeKosten]) desc;
GO


