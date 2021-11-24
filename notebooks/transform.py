# Databricks notebook source
# MAGIC %md # Init

# COMMAND ----------

def file_exists(path):
  if path[:5] == "/dbfs":
    import os
    return os.path.exists(path)
  else:
    try:
      dbutils.fs.ls(path)
      return True
    except Exception as e:
      if 'java.io.FileNotFoundException' in str(e):
        return False
      else:
        raise

# COMMAND ----------

# MAGIC %md ## config

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md ## load raw txt files

# COMMAND ----------

# MAGIC %run ./load_raw

# COMMAND ----------

# MAGIC %md # Convert

# COMMAND ----------

df_openalex_c={}

# COMMAND ----------

# MAGIC %md ## Paper

# COMMAND ----------

# create a paper dataframe that combines what is now spread across tables
df_openalex_c['paper']=(
  df_raw_import['Papers']
  .join(
    df_raw_import['PaperAbstractsInvertedIndex']
    .select(
      'PaperId',
      # uninvert the inverted index: use a udf, pass the object after parsing JSON-string
      # read the code backwards: it starts from the deepest nested code.
      # 2. the UDF
      func.udf(
        # 2.5 join the list of tokens by space
        lambda IndexedAbstract: " ".join(
          # 2.4 get a list of ordered-tokens 
          [
            # 2.3 keep only the token element of the sorted-flat-list
            idx_token[1] for idx_token in sorted(
              # 2.2b sort the index-token list so the tokens are in order
              [
                # 2.1 convert inverted index into a flat list of list: [index,token]
                [index,token] for token in IndexedAbstract['InvertedIndex'] for index in IndexedAbstract['InvertedIndex'][token]
              ],
              # 2.2a sort the index-token list by the first element of each nested array (pair of idx and key)
              key=lambda idx_token: idx_token[0]
            )
          ]
        )
      )
      # 1. the JSON string being read and parsed, then passed to above UDF.
      (
        # convert the string JSON inverted index into a struct<[long],[map]>
        func.from_json(
          # address issue with double escape characters around double quotes.
          func.regexp_replace(func.col('IndexedAbstract'),"\\\\\\\\","\\\\"),
          # structure of the JSON / result
          StructType([
            StructField("IndexLength",LongType(),False),
            StructField(
              "InvertedIndex",
              MapType(StringType(),ArrayType(LongType()),False)
            )
          ])
        )
      )
      .alias('Abstract')
    )
    ,
    ['PaperId'],'LEFT_OUTER'
  ) 
  .join(
    df_raw_import['PaperAuthorAffiliations']
    .groupBy('PaperId','AffiliationId','OriginalAffiliation')
    # a single OriginalAffiliation string (when NULL) can have multiple AffiliationIds, 
    # and a single AffiliationId can have multiple OriginalAffiliations on the paper
    .agg(
      func.collect_set(func.struct('AuthorSequenceNumber','AuthorId','OriginalAuthor')).alias('Authors')
    )
    .groupBy('PaperId')
    .agg(
      func.collect_list(
        func.struct('AffiliationId','OriginalAffiliation','Authors')
      ).alias('AuthorAffiliations')
    ),
    ['PaperId'],'LEFT_OUTER'
  )
  .join(
    df_raw_import['PaperCitationContexts']
    .groupBy('PaperId')
    .agg(
      func.collect_list(
        func.struct('PaperReferenceId','CitationContext')
      ).alias('CitationContexts')
    ),
    ['PaperId'],'LEFT_OUTER'
  )
  .join(
    df_raw_import['PaperExtendedAttributes']
    .groupBy('PaperId')
    .agg(
      func.collect_list(
        func.struct('AttributeType','AttributeValue')
      ).alias('ExtendedAttributes')
    )
    # parse extended attributes into column semantics.
    # 1=PatentId, 2=PubMedId, 3=PmcId, 4=Alternative Title
    # PatentId not present in data.
    .withColumn(
      'PubMedId',
      func.expr('filter(ExtendedAttributes,x->x.AttributeType==2).AttributeValue')
    )
    .withColumn(
      'PmcId',
      func.expr('filter(ExtendedAttributes,x->x.AttributeType=3).AttributeValue')
    )
    .withColumn(
      'AlternativeTitle',
      func.expr('filter(ExtendedAttributes,x->x.AttributeType=4).AttributeValue')
    )
    .drop('ExtendedAttributes')      
    ,
    ['PaperId'],'LEFT_OUTER'
  ) 
  .join(
    df_raw_import['PaperFieldsOfStudy']
    .groupBy('PaperId')
    .agg(
      func.collect_list(
        func.struct('FieldOfStudyId','Score','AlgorithmVersion')
      ).alias('FieldsOfStudy')
    ),
    ['PaperId'],'LEFT_OUTER'
  )  
  .join(
    df_raw_import['PaperMeSH']
    .groupBy('PaperId')
    .agg(
      func.collect_list(
        func.struct('DescriptorUI','DescriptorName','QualifierUI','QualifierName','IsMajorTopic')
      ).alias('MeSH')
    ),
    ['PaperId'],'LEFT_OUTER'
  )
  .join(
    df_raw_import['PaperRecommendations']
    .groupBy('PaperId')
    .agg(
      func.collect_list(
        func.struct('RecommendedPaperId','Score')
      ).alias('Recommendations')
    ),
    ['PaperId'],'LEFT_OUTER'
  )
  .join(
    df_raw_import['PaperReferences']
    .groupBy('PaperId')
    .agg(
      func.collect_list(
        'PaperReferenceId'
      ).alias('References')
    ),
    ['PaperId'],'LEFT_OUTER'
  )
  
  .join(
    df_raw_import['PaperResources']
    .groupBy('PaperId')
    .agg(
      func.collect_list(
        func.struct('ResourceType','ResourceUrl','SourceUrl')
      ).alias('Resources')
    ),
    ['PaperId'],'LEFT_OUTER'
  )
  
  .join(
    df_raw_import['PaperUrls']
    .groupBy('PaperId')
    .agg(
      func.collect_list(
        func.struct('SourceType','SourceUrl','LanguageCode','UrlForLandingPage')
      ).alias('Urls')
    ),
    ['PaperId'],'LEFT_OUTER'
  )
)


# COMMAND ----------

# MAGIC %md ## FieldOfStudy

# COMMAND ----------

# normalize all ancestors of a field into a dataframe, 
# for joining later. This is relatively expensive to do, 
# and not very expensive to store, so doing it once and 
# storing the result is a way to make subject aggregations
# less expensive later.
df_fos_ancestor_family=(
  df_raw_import['FieldsOfStudy']
  .select('FieldOfStudyId',func.struct('Level','FieldOfStudyId').alias('item'))
  
  # get parent 1
  .join(
    df_raw_import['FieldOfStudyChildren']
    .select(
      func.col('FieldOfStudyId').alias('parent_1_FieldOfStudyId'),
      func.col('ChildFieldOfStudyId').alias('FieldOfStudyId')
    )
    .join(
      df_raw_import['FieldsOfStudy']
      .select(func.col('FieldOfStudyId').alias('parent_1_FieldOfStudyId'),func.struct('Level','FieldOfStudyId').alias('parent_1'))
      ,['parent_1_FieldOfStudyId'],'INNER' 
    ),
    ['FieldOfStudyId'],'LEFT_OUTER'
  )
  # get parent 2
  .join(
    df_raw_import['FieldOfStudyChildren']
    .select(
      func.col('FieldOfStudyId').alias('parent_2_FieldOfStudyId'),
      func.col('ChildFieldOfStudyId').alias('parent_1_FieldOfStudyId')
    )
    .join(
      df_raw_import['FieldsOfStudy']
      .select(func.col('FieldOfStudyId').alias('parent_2_FieldOfStudyId'),func.struct('Level','FieldOfStudyId').alias('parent_2'))
      ,['parent_2_FieldOfStudyId'],'INNER'
    ),
    ['parent_1_FieldOfStudyId'],'LEFT_OUTER'
  )
  # get parent 3
  .join(
    df_raw_import['FieldOfStudyChildren']
    .select(
      func.col('FieldOfStudyId').alias('parent_3_FieldOfStudyId'),
      func.col('ChildFieldOfStudyId').alias('parent_2_FieldOfStudyId')
    )
    .join(
      df_raw_import['FieldsOfStudy']
      .select(func.col('FieldOfStudyId').alias('parent_3_FieldOfStudyId'),func.struct('Level','FieldOfStudyId').alias('parent_3'))
      ,['parent_3_FieldOfStudyId'],'INNER'
    ),
    ['parent_2_FieldOfStudyId'],'LEFT_OUTER'
  )
  # get parent 4
  .join(
    df_raw_import['FieldOfStudyChildren']
    .select(
      func.col('FieldOfStudyId').alias('parent_4_FieldOfStudyId'),
      func.col('ChildFieldOfStudyId').alias('parent_3_FieldOfStudyId')
    )
    .join(
      df_raw_import['FieldsOfStudy']
      .select(func.col('FieldOfStudyId').alias('parent_4_FieldOfStudyId'),func.struct('Level','FieldOfStudyId').alias('parent_4'))
      ,['parent_4_FieldOfStudyId'],'INNER'
    ),
    ['parent_3_FieldOfStudyId'],'LEFT_OUTER'
  )
  # get parent 5
  .join(
    df_raw_import['FieldOfStudyChildren']
    .select(
      func.col('FieldOfStudyId').alias('parent_5_FieldOfStudyId'),
      func.col('ChildFieldOfStudyId').alias('parent_4_FieldOfStudyId')
    )
    .join(
      df_raw_import['FieldsOfStudy']
      .select(func.col('FieldOfStudyId').alias('parent_5_FieldOfStudyId'),func.struct('Level','FieldOfStudyId').alias('parent_5'))
      ,['parent_5_FieldOfStudyId'],'INNER'
    ),
    ['parent_4_FieldOfStudyId'],'LEFT_OUTER'
  )
  
  # because some fields have multiple parents, we need to group.
  .groupBy('FieldOfStudyId')
  .agg(
    # FieldAncestorTree is a list of structs, where each struct
    # consists of 'Level' and 'FieldOfStudyId'
    # it has the item itself and all of its ancestors (parents / 
    # grandparents etc)
    func.array_distinct(
      func.flatten(
        func.collect_list(
          func.array_except(
            func.array('item','parent_1','parent_2','parent_3','parent_4','parent_5'),
            func.array(func.lit(None))
          )
        )
      )
    ).alias('FieldAncestorTree')
  )
)

# COMMAND ----------

df_openalex_c['fieldofstudy']=(
  df_raw_import['FieldsOfStudy']
  .join(
    df_fos_ancestor_family,['FieldOfStudyId'],'LEFT_OUTER'
  )
  .join(
    df_raw_import['FieldOfStudyChildren']
    .groupBy('FieldOfStudyId')
    .agg(func.collect_list('ChildFieldOfStudyId').alias('ChildFieldOfStudyIds'))
    ,['FieldOfStudyId'],
    'LEFT_OUTER'
  )
  
  .join(
    df_raw_import['FieldOfStudyExtendedAttributes']
    .groupBy('FieldOfStudyId')
    .agg(
      func.collect_list(
        func.struct('AttributeType','AttributeValue')
      ).alias('ExtendedAttributes')
    )
    # parse extended attributes into column semantics.
    # 1 (AUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsarchives04.html#2018AA), 
    # 2 (source url), 
    # 3 (CUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html)
    # multi valued (observed)
    .withColumn(
      'AUI',
      func.expr('filter(ExtendedAttributes,x->x.AttributeType=1).AttributeValue')
    )
    # we only see one value in the data now, but technically this could be multi-valued.
    .withColumn(
      'sourceurl',
      func.expr('filter(ExtendedAttributes,x->x.AttributeType=2).AttributeValue')
    )
    # we only see one value in the data now, but technically this could be multi-valued.
    .withColumn(
      'CUI',
      func.expr('filter(ExtendedAttributes,x->x.AttributeType=3).AttributeValue')
    )
    .drop('ExtendedAttributes')      
    ,
    ['FieldOfStudyId'],'LEFT_OUTER'
  ) 
  
)

# COMMAND ----------

# MAGIC %md ## Journal

# COMMAND ----------

df_openalex_c['journal']=(
  df_raw_import['Journals']
  .withColumn('Issns',func.from_json(func.col('Issns'),ArrayType(StringType())))
)

# COMMAND ----------

# MAGIC %md ## Author

# COMMAND ----------

df_openalex_c['author']=(
  df_raw_import['Authors']
  .join(
    df_raw_import['AuthorExtendedAttributes']
    .groupBy('AuthorId')
    .agg(
      func.collect_list(
        func.struct('AttributeType','AttributeValue')
      ).alias('ExtendedAttributes')
    )
    # parse extended attributes into column semantics.
    # 1=Alternative name
    .withColumn(
      'AlternativeNames',
      func.expr('filter(ExtendedAttributes,x->x.AttributeType=1).AttributeValue')
    )
    .drop('ExtendedAttributes'),
    ['AuthorId'],'LEFT_OUTER'
  )
)


# COMMAND ----------

# MAGIC %md ## Affiliation

# COMMAND ----------

df_openalex_c['affiliation']=(
  df_raw_import['Affiliations']
)
