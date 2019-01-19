package org.diabetes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection

import org.apache.spark.sql.types.StructType

//Diabetes case class
case class diabetesCase(encounter_id:String,patient_nbr:String,race:String,gender:String,age:String,weight:String,admission_type_id:String,
                        discharge_disposition_id:String,admission_source_id:String,time_in_hospital:String,payer_code:String,medical_speciality:String,
                        num_lab_procedures:String,num_procedures:String,num_medications:String,number_outpatient:String,number_emergency:String,
                        number_inpatient:String,diag_1:String,diag_2:String,diag_3:String,number_diagnoses:String,max_glu_serum:String,A1Cresult:String,metformin:String,
                        repaglinide:String,nateglinide:String,chlorpropamide:String,glimepiride:String, acetohexamide:String,glipizide:String,glyburide:String,
                        tolbutamide:String,pioglitazone:String,rosiglitazone:String,acarbose:String,miglitol:String,troglitazone:String,tolazamide:String,
                        examide:String,citoglipton:String,insulin:String,glyburide_metformin:String, glipizide_metformin:String, glimepiride_pioglitazone:String,
                        metformin_rosiglitazone:String,metformin_pioglitazone:String,change:String,diabetesMed:String,readmitted:String )

//Discharge case class
case class dischargeCase(discharge_disposition_id:String,description:String)


//Admission case class
case class admissionSourceCase(admission_source_id:String,description:String)

//Admission type case class
case class admissionTypeCase(admission_type_id:String,description:String)





//define spark session wrapper as a trait for extension purposes
trait sessionWrapper {
  lazy val spark: SparkSession = SparkSession.builder().master("local[*]").appName("diabetesCal").getOrCreate()

//Defining all the schemas
  val diabSchema: StructType = ScalaReflection.schemaFor[diabetesCase].dataType.asInstanceOf[StructType]
  val dischargeSchema: StructType = ScalaReflection.schemaFor[dischargeCase].dataType.asInstanceOf[StructType]
  val admissionSourceSchema: StructType = ScalaReflection.schemaFor[admissionSourceCase].dataType.asInstanceOf[StructType]
  val admissionTypeSchema: StructType = ScalaReflection.schemaFor[admissionTypeCase].dataType.asInstanceOf[StructType]

  def buildDF(path:String,datafile: String,passSchema:StructType): DataFrame = {
    spark.read.format("csv").option("header",value = true).schema(passSchema).load(path+"/"+datafile).cache()
  }


}