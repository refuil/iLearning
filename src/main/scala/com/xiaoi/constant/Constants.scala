package com.xiaoi.constant

/**
  * Constants Interface
  * Created by josh.ye on 6/19/17.
  */
object Constants {
  /**
    * Project Config
    */
  val DATABASE = "database"
  val JDBC_DRIVER = "jdbc.driver"
  val JDBC_DATASOURCE_SIZE = "jdbc.datasource.size"
  val JDBC_URL = "jdbc.url"
  val JDBC_USER = "jdbc.user"
  val JDBC_PASSWORD = "jdbc.password"
  val JDBC_URL_PROD = "jdbc.url.prod"
  val JDBC_USER_PROD = "jdbc.user.prod"
  val JDBC_PASSWORD_PROD = "jdbc.password.prod"

  /**
    * ETL
    */
  val IMPORT_DAY = "import"
  val OLD_IMPORT = "old_import"

  /**
    * Spark Job
    */
  val SPARK_LOCAL = "spark.local"
  val SPARK_MASTER_URL = "spark://master:7077"
  val SPARK_LOCAL_TASKID_UNANSWERED_ANALYSIS = "spark.local.taskid.unanswered_analysis"

  /**
    * Special Character
    */
  val STOP_WORDS="stop_words"

  /**
    * Robot Field Type
    */
  val RESULT_TYPE_NULL = "result_type_null"
  val RESULT_TYPE_FINAL = "result_type_final"
  val RESULT_TYPE_FINAL_LIST = "result_type_list"
  val RESULT_TYPE_MISINPUT = "result_type_misInput"
  val RESULT_TYPE_COMMONCHAT = "result_type_commonChat"
  val RESULT_TYPE_ILLEGALWORD = "result_type_illegalWord"
  val RESULT_TYPE_REPEAT = "result_type_repeat"
  val RESULT_TYPE_SIMPLE = "result_type_simple"
  val RESULT_TYPE_CUSTOMCHAT = "result_type_customChat"
  val RESULT_TYPE_EXPIRED = "result_type_expired"
  val RESULT_TYPE_WRONGSPELL = "result_type_wrongSpell"
  val RESULT_TYPE_SUGGESTED = "result_type_suggested"
  val RESULT_TYPE_RELATEDCOMMAND = "result_type_relatedCommand"
  val RESULT_TYPE_GROUPQUESTION = "result_type_groupQuestion"
  val RESULT_TYPE_REQUESTlIMITED = "result_type_requestLimited"
  val RESULT_TYPE_SESSIONLIMITED = "result_type_sessionLimited"

  /**
    * Robot log field after ETL
    */
  val FIELD_VISIT_TIME = "field_visit_time"
  val FIELD_SESSION_ID = "field_session_id"
  val FIELD_USER_ID = "field_user_id"
  val FIELD_QUESTION = "field_question"
  val FIELD_QUESTION_TYPE = "field_question_type"
  val FIELD_ANSWER = "field_answer"
  val FIELD_ANSWER_TYPE = "field_answer_type"
  val FIELD_FAQ_ID = "field_faq_id"
  val FIELD_FAQ_NAME = "field_faq_name"
  val FIELD_KEYWORD = "field_keyword"
  val FIELD_CITY = "field_city"
  val FIELD_BRAND = "field_brand"
  val FIELD_SIMILARITY = "field_similarity"
  val FIELD_MODULE_ID = "field_module_id"
  val FIELD_PLATFORM= "field_platform"
  val FIELD_EX= "field_ex"
  val FIELD_CATEGORY = "field_category"
  val FIELD_NOPUNCTUATION = "field_nopunctuation"
  val FIELD_SEGMENT = "field_segment"
  val FIELD_SENTIMENT = "field_sentiment"

  /**
    * Weight Constants
    */
  val WEIGHT_W_FACTOR="W_FACTOR"
  val WEIGHT_M_FACTOR="M_FACTOR"
  val WEIGHT_R_FACTOR="R_FACTOR"
  val WEIGHT_TYPE_0="TYPE_0"
  val WEIGHT_TYPE_11="TYPE_11"
  val WEIGHT_MANUAL="MANUAL"
  val WEIGHT_MAX_R_WEI="MAX_R_WEI"
  val WEIGHT_CHAT_WEI_N1="CHAT_WEI_N1"
  val WEIGHT_CHAT_WEI_0="CHAT_WEI_0"
  val WEIGHT_CHAT_WEI_1="CHAT_WEI_1"
  val WEIGHT_CHAT_WEI_2="CHAT_WEI_2"
  val WEIGHT_SUGGEST_WEI="SUGGEST_WEI"
  val WEIGHT_MAN_WEI="MAN_WEI"
  val WEIGHT_SUGGEST_RATIO="SUGGEST_RATIO"
  val WEIGHT_PLAT_WEI="PLAT_WEI"

//新词发现
  val FIND_NEW_WORD = "findNewWord"


}
