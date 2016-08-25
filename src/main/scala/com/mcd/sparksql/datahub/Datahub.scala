package com.mcd.sparksql.datahub
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType };
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.mcd.sparksql.datahub._
object Datahub {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Spark SQL Application With RDD To DF").setMaster("local[2]").set("spark.executor.memory", "1g")
    // sc is an existing SparkContext.
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    
    
    
    val customDatahubSchema=StructType(Array(
StructField("posbusndt", StringType, true),
StructField("pos_ord_key_id", StringType, true),
StructField("lgcy_lcl_rfr_def_cd", StringType, true),
StructField("mcd_gbal_lcat_id_nu", StringType, true),
StructField("terrcd", StringType, true),
StructField("pos_evnt_typ_cd", StringType, true),
StructField("pos_area_typ_shrt_ds", StringType, true),
StructField("pos_trn_strt_ts", StringType, true),
StructField("pos_ord_uniq_id", StringType, true),
StructField("pos_trn_typ_cd", StringType, true),
StructField("pos_paid_dvce_id", StringType, true),
StructField("pos_mfy_side_cd", StringType, true),
StructField("pos_prd_dlvr_meth_cd", StringType, true),
StructField("pos_tot_net_trn_am", StringType, true),
StructField("pos_tot_grss_trn_am", StringType, true),
StructField("pos_tot_nprd_net_trn_am", StringType, true),
StructField("pos_tot_nprd_grss_trn_am", StringType, true),
StructField("pos_tot_tax_am", StringType, true),
StructField("pos_tot_nprd_tax_am", StringType, true),
StructField("pos_ord_strt_ts", StringType, true),
StructField("pos_ord_end_ts", StringType, true),
StructField("sld_menu_itm_id", StringType, true),
StructField("pos_itm_prd_typ_cd", StringType, true),
StructField("pos_itm_actn_cd", StringType, true),
StructField("pos_itm_sds", StringType, true),
StructField("pos_itm_lvl_nu", StringType, true),
StructField("pos_itm_seq_nu", StringType, true),
StructField("pos_itm_line_seq_nu", StringType, true),
StructField("pos_itm_tot_qt", StringType, true),
StructField("pos_itm_grll_qt", StringType, true),
StructField("pos_itm_grll_mod_cd", StringType, true),
StructField("pos_itm_prmo_qt", StringType, true),
StructField("pos_itm_chg_aft_tot_fl", StringType, true),
StructField("pos_itm_bp_prc_am", StringType, true),
StructField("pos_itm_bp_tax_am", StringType, true),
StructField("pos_itm_bd_prc_am", StringType, true),
StructField("pos_itm_bd_tax_am", StringType, true),
StructField("pos_itm_unt_prc", StringType, true),
StructField("pos_itm_unt_tax_am", StringType, true),
StructField("pos_itm_grss_tot_prc_am", StringType, true),
StructField("pos_itm_net_tot_prc_am", StringType, true),
StructField("pos_itm_tot_tax_am", StringType, true),
StructField("pos_itm_net_unt_prc_am", StringType, true),
StructField("pos_itm_grss_unt_prc_am", StringType, true),
StructField("pos_itm_net_tot_am", StringType, true),
StructField("pos_itm_grss_tot_am", StringType, true),
StructField("pos_itm_tax_pc", StringType, true),
StructField("pos_itm_net_prmo_am", StringType, true),
StructField("pos_itm_grss_prmo_am", StringType, true),
StructField("pos_itm_net_prc_disc_am", StringType, true),
StructField("pos_itm_grss_prc_disc_am", StringType, true),
StructField("pos_itm_net_tot_disc_am", StringType, true),
StructField("pos_itm_grss_tot_disc_am", StringType, true),
StructField("pos_itm_orgn_net_unt_prc_am", StringType, true),
StructField("pos_itm_orgn_grss_unt_prc_am", StringType, true),
StructField("pos_itm_cat_cd", StringType, true),
StructField("pos_itm_fmly_grp_cd", StringType, true),
StructField("pos_itm_void_qt", StringType, true),
StructField("pos_itm_tax_rate_am", StringType, true),
StructField("pos_itm_tax_bas_am", StringType, true),
StructField("itm_prmo_appd_fl", StringType, true),
StructField("itm_promo_id", StringType, true),
StructField("itm_promo_nm", StringType, true),
StructField("itm_promo_qt", StringType, true),
StructField("itm_prmo_appd_digl_offr_id", StringType, true),
StructField("itm_prmo_appd_offr_cter_qt", StringType, true),
StructField("itm_prmo_appd_offr_elig_fl", StringType, true),
StructField("itm_prmo_appd_orgn_prc_am", StringType, true),
StructField("itm_prmo_appd_disc_am", StringType, true),
StructField("itm_prmo_appd_disc_typ_cd", StringType, true),
StructField("itm_prmo_appd_orgn_offr_qt", StringType, true),
StructField("itm_prmo_appd_orgn_prd_cd", StringType, true),
StructField("pos_prmo_digl_offr_id", StringType, true),
StructField("pos_prmo_cter_qt", StringType, true),
StructField("pos_prmo_disc_typ_cd", StringType, true),
StructField("pos_prmo_disc_am", StringType, true),
StructField("pos_prmo_cust_offr_id", StringType, true),
StructField("pos_prmo_xclu_fl", StringType, true),
StructField("pos_prmo_on_tend_fl", StringType, true),
StructField("pos_prmo_rtrn_val_am", StringType, true),
StructField("cust_offr_tag_id", StringType, true),
StructField("cust_offr_id", StringType, true),
StructField("cust_offr_ovrd_fl", StringType, true),
StructField("cust_offr_appd_fl", StringType, true),
StructField("cust_offr_cler_aft_ovrd_fl", StringType, true),
StructField("cust_offr_digl_offr_id", StringType, true),
StructField("1_pos_tend_typ_cd", StringType, true),
StructField("1_pos_pymt_meth_typ_cd", StringType, true),
StructField("1_pos_pymt_meth_ds_tx", StringType, true),
StructField("1_pos_tend_qt", StringType, true),
StructField("1_pos_tend_am", StringType, true),
StructField("1_pos_cshl_card_pvdr_typ_cd", StringType, true),
StructField("1_pos_cshl_data_tx", StringType, true),
StructField("1_pos_cshl_card_nu", StringType, true),
StructField("1_pos_cshl_athz_cd", StringType, true),
StructField("1_pos_cshl_pymt_am", StringType, true),
StructField("1_pos_cshl_tokn_cd", StringType, true),
StructField("1_pos_cshl_card_xpir_mo_nu", StringType, true),
StructField("1_pos_cshl_card_xpir_yr_nu", StringType, true),
StructField("2_pos_tend_typ_cd", StringType, true),
StructField("2_pos_pymt_meth_typ_cd", StringType, true),
StructField("2_pos_pymt_meth_ds_tx", StringType, true),
StructField("2_pos_tend_qt", StringType, true),
StructField("2_pos_tend_am", StringType, true),
StructField("2_pos_cshl_card_pvdr_typ_cd", StringType, true),
StructField("2_pos_cshl_data_tx", StringType, true),
StructField("2_pos_cshl_card_nu", StringType, true),
StructField("2_pos_cshl_athz_cd", StringType, true),
StructField("2_pos_cshl_pymt_am", StringType, true),
StructField("2_pos_cshl_tokn_cd", StringType, true),
StructField("2_pos_cshl_card_xpir_mo_nu", StringType, true),
StructField("2_pos_cshl_card_xpir_yr_nu", StringType, true),
StructField("3_pos_tend_typ_cd", StringType, true),
StructField("3_pos_pymt_meth_typ_cd", StringType, true),
StructField("3_pos_pymt_meth_ds_tx", StringType, true),
StructField("3_pos_tend_qt", StringType, true),
StructField("3_pos_tend_am", StringType, true),
StructField("3_pos_cshl_card_pvdr_typ_cd", StringType, true),
StructField("3_pos_cshl_data_tx", StringType, true),
StructField("3_pos_cshl_card_nu", StringType, true),
StructField("3_pos_cshl_athz_cd", StringType, true),
StructField("3_pos_cshl_pymt_am", StringType, true),
StructField("3_pos_cshl_tokn_cd", StringType, true),
StructField("3_pos_cshl_card_xpir_mo_nu", StringType, true),
StructField("3_pos_cshl_card_xpir_yr_nu", StringType, true),
StructField("4_pos_tend_typ_cd", StringType, true),
StructField("4_pos_pymt_meth_typ_cd", StringType, true),
StructField("4_pos_pymt_meth_ds_tx", StringType, true),
StructField("4_pos_tend_qt", StringType, true),
StructField("4_pos_tend_am", StringType, true),
StructField("4_pos_cshl_card_pvdr_typ_cd", StringType, true),
StructField("4_pos_cshl_data_tx", StringType, true),
StructField("4_pos_cshl_card_nu", StringType, true),
StructField("4_pos_cshl_athz_cd", StringType, true),
StructField("4_pos_cshl_pymt_am", StringType, true),
StructField("4_pos_cshl_tokn_cd", StringType, true),
StructField("4_pos_cshl_card_xpir_mo_nu", StringType, true),
StructField("4_pos_cshl_card_xpir_yr_nu", StringType, true),
StructField("5_pos_tend_typ_cd", StringType, true),
StructField("5_pos_pymt_meth_typ_cd", StringType, true),
StructField("5_pos_pymt_meth_ds_tx", StringType, true),
StructField("5_pos_tend_qt", StringType, true),
StructField("5_pos_tend_am", StringType, true),
StructField("5_pos_cshl_card_pvdr_typ_cd", StringType, true),
StructField("5_pos_cshl_data_tx", StringType, true),
StructField("5_pos_cshl_card_nu", StringType, true),
StructField("5_pos_cshl_athz_cd", StringType, true),
StructField("5_pos_cshl_pymt_am", StringType, true),
StructField("5_pos_cshl_tokn_cd", StringType, true),
StructField("5_pos_cshl_card_xpir_mo_nu", StringType, true),
StructField("5_pos_cshl_card_xpir_yr_nu", StringType, true),
StructField("pos_tot_itm_qt", StringType, true),
StructField("pos_paid_for_ord_ts", StringType, true),
StructField("pos_tot_key_prss_ts", StringType, true),
StructField("pos_ord_str_in_sys_ts", StringType, true),
StructField("pos_rduc_qt", StringType, true),
StructField("pos_rduc_aft_tot_qt", StringType, true),
StructField("pos_rduc_b4_tot_qt", StringType, true),
StructField("pos_rduc_b4_tot_am", StringType, true),
StructField("pos_rduc_am", StringType, true),
StructField("pos_rduc_aft_tot_am", StringType, true),
StructField("cust_id", StringType, true),
StructField("ord_digl_offr_appd_fl", StringType, true),
StructField("dypt_id_nu", StringType, true),
StructField("untl_tot_key_prss_sc_qt", StringType, true),
StructField("untl_str_in_sys_sc_qt", StringType, true),
StructField("untl_ord_rcll_sc_qt", StringType, true),
StructField("untl_drwr_clse_sc_qt", StringType, true),
StructField("untl_paid_sc_qt", StringType, true),
StructField("untl_srv_sc_qt", StringType, true),
StructField("tot_ord_tm_sc_qt", StringType, true),
StructField("abov_psnt_tm_trgt_fl", StringType, true),
StructField("abov_tot_tm_trgt_fl", StringType, true),
StructField("abov_tot_mfy_trgt_tm_tm_fl", StringType, true),
StructField("abov_tot_frnt_cter_trgt_tm_fl", StringType, true),
StructField("abov_tot_drv_trgt_tm_fl", StringType, true),
StructField("abov_50_sc_fl", StringType, true),
StructField("bel_25_sc_fl", StringType, true),
StructField("held_tm_sc_qt", StringType, true),
StructField("ord_held_fl", StringType, true),
StructField("pos_tm_prod_node_na", StringType, true),
StructField("untl_ord_prep_sc_qt", StringType, true),
StructField("mfy_tm_prod_node_na", StringType, true),
StructField("mfy_untl_tot_key_prss_sc_qt", StringType, true),
StructField("mfy_untl_strd_in_sys_sc_qt", StringType, true),
StructField("mfy_untl_ord_rcll_sys_sc_qt", StringType, true),
StructField("mfy_untl_drwr_clse_sc_qt", StringType, true),
StructField("mfy_untl_paid_sc_qt", StringType, true),
StructField("mfy_untl_srv_sc_qt", StringType, true),
StructField("mfy_tot_ord_tm_sc_qt", StringType, true),
StructField("mfy_tot_itm_cnt_qt", StringType, true),
StructField("cbb_tm_prod_node_na", StringType, true),
StructField("cbb_untl_tot_key_prss_sc_qt", StringType, true),
StructField("cbb_untl_strd_in_sys_sc_qt", StringType, true),
StructField("cbb_untl_ord_rcll_sys_sc_qt", StringType, true),
StructField("cbb_untl_drwr_clse_sc_qt", StringType, true),
StructField("cbb_untl_paid_sc_qt", StringType, true),
StructField("cbb_untl_srv_sc_qt", StringType, true),
StructField("cbb_tot_ord_tm_sc_qt", StringType, true),
StructField("cbb_tot_itm_cnt_qt", StringType, true),
StructField("last_isrt_ts", StringType, true),
StructField("last_updt_ts", StringType, true),
StructField("terr_cd", StringType, true),
StructField("pos_busn_dt", StringType, true)))

import org.apache.spark.sql._
/*val datahub = sc.textFile("datahub/data")
val rowRDD = datahub.map(_.split("\t")).map(p => Row(p(0), p(1).trim))

val datahubDataFrame = sqlContext.createDataFrame(rowRDD, customDatahubSchema)

datahubDataFrame.registerTempTable("tld_datahub")

val results = sqlContext.sql("SELECT  posbusndt FROM tld_datahub")

results.map(t => "Posbusndt: " + t(0)).collect().foreach(println)*/



/*val customSchema = StructType(Array(
      StructField("year", IntegerType, true),
      StructField("make", StringType, true),
      StructField("model", StringType, true),
      StructField("comment", StringType, true),
      StructField("blank", StringType, true)))
*/
    val datahubDataFrame = sqlContext.load(
      "com.databricks.spark.csv",
      schema = customDatahubSchema,
      Map("path" -> "datahub/data,datahub/data2", "header" -> "true","delimiter" -> "\t"))

   /* val selectedData = df.select("pos_itm_lvl_nu", "last_updt_ts")
    selectedData.take(5).foreach(println)
    selectedData.save("newcars.csv", "com.databricks.spark.csv")*/
    
    datahubDataFrame.registerTempTable("tld_datahub")

  val results = sqlContext.sql("SELECT  posbusndt,pos_itm_lvl_nu,last_updt_ts FROM tld_datahub")
 // results.save("newcars.csv", "com.databricks.spark.csv")
  results.map(t => "Posbusndt: " + t(0)).collect().foreach(println)

  }
}