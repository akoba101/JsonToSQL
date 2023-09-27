DROP PROCEDURE sp_ScriptJsonNested
GO
CREATE PROCEDURE sp_ScriptJsonNested
	@JsonTable NVARCHAR(100)
	,@JsonColumn NVARCHAR(100)
	,@JsonType INT
	,@SqlJsonNested NVARCHAR(MAX) OUTPUT
AS
BEGIN
DECLARE @SqlProperties AS NVARCHAR(500)

DECLARE @TempProperties TABLE(
[key] NVARCHAR(100)
,[type] INT
,CharLength INT
)

IF @JsonType = 5
	BEGIN
		SET @SqlProperties =	
			N'WITH CTE AS(
			SELECT 
				[key]
				, [type]
				, MAX(LEN([value])) CharLength
			FROM ' + @JsonTable + '
			CROSS APPLY OPENJSON(' + @JsonColumn + ')
			GROUP BY [key], [type]
			)
			SELECT 
				[key]
				,[type]
				,CharLength
			FROM CTE'
	END
IF @JsonType = 4
	BEGIN
		SET @SqlProperties =	
			N'WITH CTE AS(
			SELECT 
				jc2.[key]
				, jc2.[type]
				, MAX(LEN(jc2.[value])) CharLength
			FROM ' + @JsonTable + '
			CROSS APPLY OPENJSON(' + @JsonColumn + ')
			WITH([value] NVARCHAR(MAX) ''$'' AS JSON) jc
			CROSS APPLY OPENJSON(jc.[value], ''$'') jc2
			GROUP BY jc2.[key], jc2.[type]
			)
			SELECT 
				[key]
				,[type]
				,CharLength
			FROM CTE'
	END
INSERT INTO @TempProperties EXEC sp_executesql @SqlProperties
DECLARE @AltJsonCheck INT
SELECT @AltJsonCheck = COUNT(*) FROM @TempProperties;

IF @AltJsonCheck = 0 AND @JsonType = 4
	BEGIN
		SET @SqlProperties =
		N'SELECT 
			''value'' [value]
			, MIN([type]) [type]
			, MAX(LEN([value])) CharLength
		FROM ' + @JsonTable + '
		CROSS APPLY OPENJSON(' + @JsonColumn + ')'		
		INSERT INTO @TempProperties EXEC sp_executesql @SqlProperties
	END

DECLARE @CreateList NVARCHAR(4000)
DECLARE @InsertList NVARCHAR(4000)
DECLARE @SelectList NVARCHAR(4000)
DECLARE @ApplyList NVARCHAR(4000)

SELECT 
	@CreateList = 
		STRING_AGG(
			CASE 
				WHEN [type] = 1 THEN CONCAT('[',[key],'] NVARCHAR(',((CharLength/10)+1)*10,')')
				WHEN [type] = 2 THEN CONCAT('[',[key],'] INT')
				WHEN [type] = 3 THEN CONCAT('[',[key],'] BIT')
				WHEN [type] = 4 THEN CONCAT('[',[key],'_J4] NVARCHAR(',((CharLength/10)+1)*10,')')
				WHEN [type] = 5 THEN CONCAT('[',[key],'_J5] NVARCHAR(',((CharLength/10)+1)*10,')')
				ELSE NULL
			END
			, CHAR(13)+CHAR(10)+','
		)
		WITHIN GROUP (ORDER BY [key])
	,@InsertList = 
		STRING_AGG(
			CASE 
				WHEN [type] = 1 THEN CONCAT('[',[key],']')
				WHEN [type] = 2 THEN CONCAT('[',[key],']')
				WHEN [type] = 3 THEN CONCAT('[',[key],']')
				WHEN [type] = 4 THEN CONCAT('[',[key],'_J4]')
				WHEN [type] = 5 THEN CONCAT('[',[key],'_J5]')
				ELSE NULL
			END
			, CHAR(13)+CHAR(10)+','
		)
		WITHIN GROUP (ORDER BY [key])
	,@SelectList = 
		STRING_AGG(
			CASE 
				WHEN @JsonType = 5 AND [type] = 1 THEN CONCAT('jc.[',[key],']')
				WHEN @JsonType = 5 AND [type] = 2 THEN CONCAT('jc.[',[key],']')
				WHEN @JsonType = 5 AND [type] = 3 THEN CONCAT('jc.[',[key],']')
				WHEN @JsonType = 5 AND [type] = 4 THEN CONCAT('jc.[',[key],']')
				WHEN @JsonType = 5 AND [type] = 5 THEN CONCAT('jc.[',[key],']')
				WHEN @JsonType = 4 AND [type] = 1 THEN CONCAT('jc2.[',[key],']')
				WHEN @JsonType = 4 AND [type] = 2 THEN CONCAT('jc2.[',[key],']')
				WHEN @JsonType = 4 AND [type] = 3 THEN CONCAT('jc2.[',[key],']')
				WHEN @JsonType = 4 AND [type] = 4 THEN CONCAT('jc2.[',[key],']')
				WHEN @JsonType = 4 AND [type] = 5 THEN CONCAT('jc2.[',[key],']')
				ELSE NULL
			END
			, CHAR(13)+CHAR(10)+','
		)
		WITHIN GROUP (ORDER BY [key])
	, @ApplyList = 
		STRING_AGG(
			CASE 
				WHEN [type] = 1 THEN CONCAT('[',[key],'] NVARCHAR(',((CharLength/10)+1)*10,') ''$.',[key],'''')
				WHEN [type] = 2 THEN CONCAT('[',[key],'] INT ''$.',[key],'''')
				WHEN [type] = 3 THEN CONCAT('[',[key],'] BIT ''$.',[key],'''')
				WHEN [type] IN(4,5) THEN CONCAT('[',[key],'] NVARCHAR(MAX) ''$.',[key],''' AS JSON')
				ELSE NULL
			END
			, CHAR(13)+CHAR(10)+','
		)
		WITHIN GROUP (ORDER BY [key])
FROM @TempProperties


IF @JsonType = 5
	SET @SQLJsonNested =
		'CREATE TABLE st' + @JsonColumn +'(' + CHAR(13)+CHAR(10) +
		'ID INT IDENTITY(1,1) PRIMARY KEY' + CHAR(13)+CHAR(10) +
		',CardID INT' + CHAR(13)+CHAR(10) +
		',' + @CreateList + CHAR(13)+CHAR(10) +
		');'+ CHAR(13)+CHAR(10) +
		'GO' + CHAR(13)+CHAR(10) +
		'INSERT INTO st' + @JsonColumn +'(' + CHAR(13)+CHAR(10) +
		'CardID' + CHAR(13)+CHAR(10) +
		',' + @InsertList + CHAR(13)+CHAR(10) +
		')'+ CHAR(13)+CHAR(10) +
		'SELECT' + CHAR(13)+CHAR(10) +
		'ID' + CHAR(13)+CHAR(10) +
		',' + @SelectList  + CHAR(13)+CHAR(10) +
		'FROM ' + @JsonTable + ' CROSS APPLY OPENJSON(' + @JsonColumn + ')' + CHAR(13)+CHAR(10) +
		'WITH (' + CHAR(13)+CHAR(10) +
		@ApplyList + CHAR(13)+CHAR(10) +
		') jc;'
IF @JsonType = 4 AND @AltJsonCheck <> 0
	SET @SQLJsonNested =
		'CREATE TABLE st' + @JsonColumn +'(' + CHAR(13)+CHAR(10) +
		'ID INT IDENTITY(1,1) PRIMARY KEY' + CHAR(13)+CHAR(10) +
		',CardID INT' + CHAR(13)+CHAR(10) +
		',' + @CreateList + CHAR(13)+CHAR(10) +
		');'+ CHAR(13)+CHAR(10) +
		'GO' + CHAR(13)+CHAR(10) +
		'INSERT INTO st' + @JsonColumn +'(' + CHAR(13)+CHAR(10) +
		'CardID' + CHAR(13)+CHAR(10) +
		',' + @InsertList + CHAR(13)+CHAR(10) +
		')'+ CHAR(13)+CHAR(10) +
		'SELECT' + CHAR(13)+CHAR(10) +
		'ID' + CHAR(13)+CHAR(10) +
		',' + @SelectList  + CHAR(13)+CHAR(10) +
		'FROM ' + @JsonTable + ' CROSS APPLY OPENJSON(' + @JsonColumn + ')' + CHAR(13)+CHAR(10) +
		'WITH([value] NVARCHAR(MAX) ''$'' AS JSON) jc' + CHAR(13)+CHAR(10) +
		'CROSS APPLY OPENJSON(jc.[value], ''$'')' + CHAR(13)+CHAR(10) +
		'WITH (' + CHAR(13)+CHAR(10) +
		@ApplyList + CHAR(13)+CHAR(10) +
		') jc2;'
IF @JsonType = 4 AND @AltJsonCheck = 0
	SET @SQLJsonNested =
		'CREATE TABLE st' + @JsonColumn +'(' + CHAR(13)+CHAR(10) +
		'ID INT IDENTITY(1,1) PRIMARY KEY' + CHAR(13)+CHAR(10) +
		',CardID INT' + CHAR(13)+CHAR(10) +
		',' + @CreateList + CHAR(13)+CHAR(10) +
		');'+ CHAR(13)+CHAR(10) +
		'GO' + CHAR(13)+CHAR(10) +
		'INSERT INTO st' + @JsonColumn +'(' + CHAR(13)+CHAR(10) +
		'CardID' + CHAR(13)+CHAR(10) +
		',' + @InsertList + CHAR(13)+CHAR(10) +
		')'+ CHAR(13)+CHAR(10) +
		'SELECT' + CHAR(13)+CHAR(10) +
		'ID' + CHAR(13)+CHAR(10) +
		',' + @SelectList  + CHAR(13)+CHAR(10) +
		'FROM ' + @JsonTable + ' CROSS APPLY OPENJSON(' + @JsonColumn + ') jc2;' 
END