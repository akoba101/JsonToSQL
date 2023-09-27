DROP PROCEDURE sp_ReturnCreateList
GO
CREATE PROCEDURE sp_ReturnCreateList
	 @JsonTable NVARCHAR(100)
	, @JsonColumn NVARCHAR(100)
	, @JsonType INT
	, @CreateList NVARCHAR(4000) OUTPUT
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
FROM @TempProperties
END