package edu.gatech.i3l.DBMap;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// DO NOT DELETE ORGANIZATION ID!!!

public class DBMapMain {
	Connection connExact = null;
	Connection connOMOP = null;
	Connection connRxNORM = null;

	// ConceptID for gender
	static int male_gender = 8507;
	static int female_gender = 8532;

	// Concept ID for race (LOINC)
	static long histpanic_latino = 40759172;
	static long white = 40770358;
	static long black_african_american = 40770359;
	static long amrican_indian_alaska_native = 40770360;
	static long asian = 40770361;
	static long native_hawaiian_other_pacific_islander = 40770362;
	static long other_race_alone = 40770363;
	static long other_race_two_or_more_alone = 40770364;

	// Concept ID for Ethnicity
	static long american_indian = 38003572;
	static long alaska_native = 38003573;
	static long asian_indian = 38003574;
	static long bangladeshi = 38003575;
	static long bhutanese = 38003576;
	static long burmese = 38003577;
	static long cambodian = 38003578;
	static long chinese = 38003579;
	static long taiwanese = 38003580;
	static long filipino = 38003581;
	static long hmong = 38003582;
	static long indonesian = 38003583;
	static long japanese = 38003584;
	static long korean = 38003585;
	static long laotian = 38003586;
	static long malaysian = 38003587;
	static long okinawan = 38003588;
	static long pakistani = 38003589;
	static long srilankan = 38003590;
	static long thai = 38003591;
	static long vietnamese = 38003592;
	static long lwo_jiman = 38003593;
	static long maldivian = 38003594;
	static long nepalese = 38003595;
	static long singaporean = 38003596;
	static long madagascar = 38003597;
	static long black = 38003598;
	static long african_american = 38003599;
	static long african = 38003600;
	static long bahamian = 38003601;
	static long barbadian = 38003602;
	static long dominican = 38003603;
	static long dominica_islander = 38003604;
	static long haitian = 38003605;
	static long jamaican = 38003606;
	static long tobagoan = 38003607;
	static long trinidadian = 38003608;
	static long west_indian = 38003609;
	static long polynesian = 38003610;
	static long micronesian = 38003611;
	static long melanesian = 38003612;
	static long other_pacific_islander = 38003613;
	static long european = 38003614;
	static long middle_eastern_north_african = 38003615;
	static long arab = 38003616;

	// Marital Status (SNOMED-CT)
	static long married = 40298551;
	static long single = 40298550;
	static long divorced = 40298553;
	static long widowed = 40298555;
	static long separated = 40298552;

	// condition type concept id.
	static long EHR_prob_list_entry = 38000245; // Condition Occurrence Type.
	static long primary_condition = 44786627;

	// observation type concept id.
	static long observation_numeric_result = 38000277;
	static long observation_text = 38000278;

	// place of service concept id
	static long pharmacy = 8562;
	static long office = 8940;
	static long independent_lab = 8809;
	static long independent_clinic = 8716;
	static long inpatient_hospital = 8717;
	static long outpatient_hospital = 8756;
	static long emergency = 8870;

	// vital concept_id (LOINC)
	static long weight_cid = 3013762;
	static long height_cid = 3036277;
	static long heart_rate_cid = 3027018;
	static long respiration_rate_cid = 3024171;
	static long temp_cid = 3020891;
	static long bmi_cid = 3038553;
	static long systolic_BP_cid = 3004249;
	static long diastolic_BP_cid = 3012888;
	static long systolic_diastolic_BP_cid = 40758413;

	// vital units
	static long height_in_cid = 9330; // [in_us]
	static long height_cm_cid = 8582; // cm
	static long weight_kg_cid = 9529; // kg
	static long weight_lb_cid = 8739; // [lb_us]
	static long per_min_cid = 8541; // /min
	static long temp_F_cid = 9289; // [degF]
	static long temp_C_cid = 8653; // [degC]
	static long bmi_unit_cid = 9531; // kg/m2
	static long BP_unit_cid = 8876; // mm[Hg]

	static long Lab_mg_dL = 8840; // mg/dL
	static long Lab_mg_L = 8751; // mg/L
	static long Lab_ng_dL = 8817; // ng/dL
	static long Lab_percent = 8554; // %
	static long Lab_meq_L = 9557; // meq/L or 10*-3.eq/L
	static long Lab_mL_min = 8795; // mL/min
	static long Lab_U_mL = 9684; // U/mL
	static long Lab_cells_uL = 8784; // {cells}/uL
	static long Lab_10_6_cells_uL = 44777575; // 10*6.{cells}/uL or
												// 10*12.{cells}/L
	static long Lab_g_dL = 8713; // g/dL
	static long Lab_pg_mL = 8845; // pg/mL
	static long Lab_ng_mL = 8842; // ng/mL
	static long Lab_pg = 8564; // pg
	static long Lab_uL = 8647; // /uL

	static long med_mg = 8576; // mg
	static long med_mL_kg = 9571; // mL/kg
	static long med_meq = 9551; // meq or 10*-3.eq
	static long med_h = 8505; // h
	static long med_mg_mL = 8861; // mg/mL
	static long med_ug = 9655; // ug
	static long med_iu_mL = 8985; // [IU]/mL

	// FHIR Condition-Status Codes
	static String provisional = "provisional";
	static String working = "working";
	static String confirmed = "confirmed";
	static String refuted = "refuted";
	static String entered_in_error = "entered-in-error";
	static String unknown = "unknown";

	// SNOMED Severity concept ID
	static long fatal = 4163278;
	static long severe = 4087703;
	static long moderate = 4285732;
	static long mild = 4116992;

	private void setup_database() {
		try {
			// This is a source database. Change this to import another
			// ExactData source
			connExact = DriverManager
					.getConnection("jdbc:mysql://localhost/ExactDataCancer?"
							+ "user=healthport&password=i3lworks");

			// This is a destination database
			connOMOP = DriverManager
					.getConnection("jdbc:mysql://localhost/FHIR_OMOP?"
							+ "user=healthport&password=i3lworks");

			// RxNORM database
			connRxNORM = DriverManager
					.getConnection("jdbc:mysql://localhost/RxNORM?"
							+ "user=healthport&password=i3lworks");

		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}

	}

	private void provider_map() {
		Statement stmtExact = null;
		Statement stmtOMOP = null;
		ResultSet rsExact = null;
		ResultSet rsOMOP = null;
		PreparedStatement pstmtOMOP = null;

		setup_database();

		try {
			long care_site_id = 0, new_care_site_id = 0;
			long organization_id = 0;
			long provider_id = 0;

			stmtExact = connExact.createStatement();
			stmtOMOP = connOMOP.createStatement();

			rsExact = stmtExact.executeQuery("SELECT * FROM PROVIDER");
//			rsOMOP = stmtOMOP
//					.executeQuery("SELECT CARE_SITE_ID FROM CARE_SITE ORDER BY CARE_SITE_ID DESC");
//			if (rsOMOP.next()) {
//				new_care_site_id = rsOMOP.getLong("care_site_id");
//			}

//			rsOMOP = stmtOMOP
//					.executeQuery("SELECT PROVIDER_ID FROM PROVIDER ORDER BY PROVIDER_ID DESC");
//			if (rsOMOP.next()) {
//				provider_id = rsOMOP.getLong("PROVIDER_ID");
//			}

			while (rsExact.next()) {
				String exactDataLocation = rsExact.getString("LOCATION");
				String exactDataSpecialty = rsExact.getString("SPECIALTY");

				exactDataLocation = exactDataLocation.replace("'", "''");

				// First create care site. Then provider.
				rsOMOP = stmtOMOP
						.executeQuery("SELECT * FROM CARE_SITE WHERE UPPER(CARE_SITE_SOURCE_VALUE)=UPPER('"
								+ exactDataLocation + "')");
				if (rsOMOP.next()) {
					care_site_id = rsOMOP.getLong("care_site_id");
					organization_id = rsOMOP.getLong("organization_id");
				} else {
					// Find organization.
					// Now find an organization.
					String SQL_ORG = "";
					if (SQL_ORG == "" && exactDataSpecialty != null
							&& !exactDataSpecialty.isEmpty()) {
						if (exactDataSpecialty.toLowerCase().contains(
								"emergency")) {
							SQL_ORG = "SELECT * FROM ORGANIZATION WHERE PLACE_OF_SERVICE_CONCEPT_ID = "
									+ emergency;
						} else if (exactDataSpecialty.toLowerCase().contains(
								"surgery")) {
							SQL_ORG = "SELECT * FROM ORGANIZATION WHERE PLACE_OF_SERVICE_CONCEPT_ID = "
									+ inpatient_hospital;
						}
					}

					if (SQL_ORG == "") {
						if (exactDataLocation.toLowerCase().contains("drug")
								|| exactDataLocation.toLowerCase().contains(
										"pharmacy")
								|| exactDataLocation.toLowerCase().contains(
										"cvs")) {
							SQL_ORG = "SELECT * FROM ORGANIZATION WHERE PLACE_OF_SERVICE_CONCEPT_ID = "
									+ pharmacy;
						} else if (exactDataLocation.toLowerCase().contains(
								"lab")) {
							SQL_ORG = "SELECT * FROM ORGANIZATION WHERE PLACE_OF_SERVICE_CONCEPT_ID = "
									+ independent_lab;
						} else if (exactDataLocation.toLowerCase().contains(
								"hospital")
								|| exactDataLocation.toLowerCase().contains(
										"medical center")) {
							SQL_ORG = "SELECT * FROM ORGANIZATION WHERE PLACE_OF_SERVICE_CONCEPT_ID = "
									+ inpatient_hospital;
						} else if (exactDataLocation.toLowerCase().contains(
								"clinic")) {
							SQL_ORG = "SELECT * FROM ORGANIZATION WHERE PLACE_OF_SERVICE_CONCEPT_ID = "
									+ independent_clinic;
						} else {
							SQL_ORG = "SELECT * FROM ORGANIZATION WHERE PLACE_OF_SERVICE_CONCEPT_ID = "
									+ office;
						}
					}

					rsOMOP = stmtOMOP.executeQuery(SQL_ORG);
					if (rsOMOP.next()) {
						organization_id = rsOMOP.getLong("organization_id");
						long place_of_service_concept_id = rsOMOP
								.getLong("PLACE_OF_SERVICE_CONCEPT_ID");
						String place_of_service_source = rsOMOP
								.getString("PLACE_OF_SERVICE_SOURCE_VALUE");

						ResultSet rsCheck = stmtOMOP
								.executeQuery("SELECT * FROM CARE_SITE WHERE ORGANIZATION_ID="
										+ organization_id
										+ " AND PLACE_OF_SERVICE_CONCEPT_ID="
										+ place_of_service_concept_id
										+ " AND CARE_SITE_SOURCE_VALUE='"
										+ exactDataLocation + "'");
						if (rsCheck.next()) {
							care_site_id = rsCheck.getLong("care_site_id");
							System.out.println("Care Site (org_id="
									+ organization_id
									+ ", place_of_service_concept_id="
									+ place_of_service_concept_id
									+ ", care_site_source=" + exactDataLocation
									+ ") exists");
						} else {
//							care_site_id = ++new_care_site_id;
							String SQL_CARE_SITE_INSERT = "INSERT INTO CARE_SITE "
									+ "(ORGANIZATION_ID, PLACE_OF_SERVICE_CONCEPT_ID, CARE_SITE_SOURCE_VALUE, PLACE_OF_SERVICE_SOURCE_VALUE) VALUES "
									+ "(?, ?, ?, ?)";
							pstmtOMOP = connOMOP
									.prepareStatement(SQL_CARE_SITE_INSERT, Statement.RETURN_GENERATED_KEYS);
							pstmtOMOP.setLong(1, organization_id);
							pstmtOMOP.setLong(2, place_of_service_concept_id);
							// if (exactDataLocation.length()>50) {
							// exactDataLocation =
							// exactDataLocation.substring(0, 49);
							// }
							pstmtOMOP.setString(3, exactDataLocation);
							pstmtOMOP.setString(4, place_of_service_source);

							pstmtOMOP.executeUpdate();

							ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
							tableKeys.next();
							care_site_id = tableKeys.getInt(1);
//							care_site_id = new_care_site_id;
							
							System.out.println("Adding New Care Site ("
									+ care_site_id + ") for care site="
									+ exactDataLocation);
							
						}

					} else {
						System.out
								.println("Failed to add Care Site because organization_id could not be obtained.");
						continue;
					}
				}

				// Now we have care_site_id and organization_id. Now create
				// provider_id.
				String provider_npi = rsExact.getString("PROVIDER_NPI");
				rsOMOP = stmtOMOP
						.executeQuery("SELECT * FROM PROVIDER WHERE NPI="
								+ rsExact.getString("PROVIDER_NPI"));
				if (rsOMOP.next()) {
					System.out.println("current provider (npi=" + provider_npi
							+ ") exists.");
					continue;
				}

				String SQL_PROVIDER_INSERT = "INSERT INTO PROVIDER "
						+ "(NPI, SPECIALTY_CONCEPT_ID, CARE_SITE_ID, PROVIDER_SOURCE_VALUE, SPECIALTY_SOURCE_VALUE) VALUES "
						+ "(?, ?, ?, ?, ?)";

				pstmtOMOP = connOMOP.prepareStatement(SQL_PROVIDER_INSERT, Statement.RETURN_GENERATED_KEYS);
				// provider_id++;
//				pstmtOMOP.setLong(1, provider_id);
				pstmtOMOP.setLong(1, Long.parseLong(provider_npi));

				// get specialty concept code.
				String ExactSpecialty = rsExact.getString("SPECIALTY");
				if (ExactSpecialty != null) {
					if (ExactSpecialty.trim().equalsIgnoreCase(
							"OTORHINOLARYNGOLOGY")) {
						ExactSpecialty = "Otolaryngology";
					} else if (ExactSpecialty.trim().equalsIgnoreCase(
							"THERAPY, PHYSICAL")) {
						ExactSpecialty = "Physical Therapy";
					} else if (ExactSpecialty.trim().equalsIgnoreCase(
							"ORTHOPEDICS")) {
						ExactSpecialty = "Orthopedic Surgery";
					} else if (ExactSpecialty.trim().equalsIgnoreCase(
							"ONCOLOGY")) {
						ExactSpecialty = "Medical Oncology";
					} else if (ExactSpecialty.trim().equalsIgnoreCase("OB/GYN")) {
						ExactSpecialty = "Obstetrics/Gynecology";
					} else if (ExactSpecialty.trim().equalsIgnoreCase(
							"FAMILY PRACTICE/PRIMARY CARE")) {
						ExactSpecialty = "Family Practice";
					}
				}

				if (ExactSpecialty != null && !ExactSpecialty.isEmpty()) {
					rsOMOP = stmtOMOP
							.executeQuery("SELECT * FROM CONCEPT WHERE VOCABULARY_ID=48 AND CONCEPT_NAME LIKE '%"
									+ ExactSpecialty + "%'");
					if (rsOMOP.next()) {
						pstmtOMOP.setLong(2, rsOMOP.getLong("CONCEPT_ID"));
						pstmtOMOP
								.setString(5, rsOMOP.getString("CONCEPT_NAME"));
					} else {
						pstmtOMOP.setNull(2, Types.NULL);
						pstmtOMOP.setNull(5, Types.NULL);
					}
				} else {
					pstmtOMOP.setNull(2, Types.NULL);
					pstmtOMOP.setNull(5, Types.NULL);
				}
				pstmtOMOP.setLong(3, care_site_id);
				pstmtOMOP.setString(4, rsExact.getString("Provider_ID"));
				pstmtOMOP.executeUpdate();

				ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
				tableKeys.next();
				provider_id = tableKeys.getInt(1);

				System.out.println("New provider (" + provider_id
						+ ") is added.");


				pstmtOMOP = connOMOP
						.prepareStatement("INSERT INTO F_PROVIDER (PROVIDER_ID, NAME, YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH, GENDER_CONCEPT_ID) VALUES "
								+ "(?,?,?,?,?,?)");
				pstmtOMOP.setLong(1, provider_id);
				pstmtOMOP.setString(2, rsExact.getString("NAME"));
				String DOB = rsExact.getString("DOB");
				String DOBs[] = DOB.split("\\-");
				pstmtOMOP.setInt(3, Integer.parseInt(DOBs[0]));
				pstmtOMOP.setInt(4, Integer.parseInt(DOBs[1]));
				pstmtOMOP.setInt(5, Integer.parseInt(DOBs[2]));

				if (rsExact.getString("SEX").equalsIgnoreCase("male")) {
					pstmtOMOP.setLong(6, male_gender);
				} else {
					pstmtOMOP.setLong(6, female_gender);
				}
				pstmtOMOP.executeUpdate();

				pstmtOMOP.close();
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				stmtExact.close();
				stmtOMOP.close();

				connExact.close();
				connOMOP.close();
			} catch (SQLException e) {

				e.printStackTrace();
			}
		}

	}

	private void encounters_map() {
		Statement stmtExact = null;
		Statement stmtOMOP = null;
		ResultSet rsExact = null;
		ResultSet rsOMOP = null;
		PreparedStatement pstmtOMOPEpisode;
		PreparedStatement pstmtOMOPVisit;
		PreparedStatement pstmtOMOPVisitF;
		PreparedStatement pstmtOMOPCondition1;
		PreparedStatement pstmtOMOPConditionF1;
		PreparedStatement pstmtOMOPCondition2;
		PreparedStatement pstmtOMOPConditionF2;

		long visit_occurrence_id = 0;

		setup_database();

		try {
			stmtExact = connExact.createStatement();
			stmtOMOP = connOMOP.createStatement();

			pstmtOMOPEpisode = connOMOP
					.prepareStatement("INSERT INTO EPISODE_OF_CARE "
							+ "(PERSON_ID, EPISODE_SOURCE_VALUE) VALUES "
							+ "(?, ?)", Statement.RETURN_GENERATED_KEYS);
			pstmtOMOPVisit = connOMOP
					.prepareStatement("INSERT INTO VISIT_OCCURRENCE "
							+ "(PERSON_ID, VISIT_START_DATE, VISIT_END_DATE, PLACE_OF_SERVICE_CONCEPT_ID, CARE_SITE_ID, PLACE_OF_SERVICE_SOURCE_VALUE) VALUES "
							+ "(?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
			pstmtOMOPVisitF = connOMOP
					.prepareStatement("INSERT INTO F_VISIT_OCCURRENCE "
							+ "(VISIT_OCCURRENCE_ID, EPISODE_OF_CARE_ID, NOTE) VALUES "
							+ "(?, ?, ?)");
			pstmtOMOPCondition1 = connOMOP
					.prepareStatement("INSERT INTO CONDITION_OCCURRENCE"
							+ " (PERSON_ID, CONDITION_CONCEPT_ID, CONDITION_START_DATE, CONDITION_END_DATE, CONDITION_TYPE_CONCEPT_ID, ASSOCIATED_PROVIDER_ID, VISIT_OCCURRENCE_ID, CONDITION_SOURCE_VALUE) VALUES"
							+ " (?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
			pstmtOMOPConditionF1 = connOMOP
					.prepareStatement("INSERT INTO F_CONDITION_OCCURRENCE (CONDITION_OCCURRENCE_ID, FHIR_CONDITION_STATUS_CODE, FHIR_CONDITION_DISPLAY, CONDITION_SEVERITY_CONCEPT_ID) VALUES"
							+ "(?, ?, ?, ?)");
			pstmtOMOPCondition2 = connOMOP
					.prepareStatement("INSERT INTO CONDITION_OCCURRENCE"
							+ " (PERSON_ID, CONDITION_CONCEPT_ID, CONDITION_START_DATE, CONDITION_END_DATE, CONDITION_TYPE_CONCEPT_ID, ASSOCIATED_PROVIDER_ID, VISIT_OCCURRENCE_ID, CONDITION_SOURCE_VALUE) VALUES"
							+ " (?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
			pstmtOMOPConditionF2 = connOMOP
					.prepareStatement("INSERT INTO F_CONDITION_OCCURRENCE (CONDITION_OCCURRENCE_ID, FHIR_CONDITION_STATUS_CODE, FHIR_CONDITION_DISPLAY, CONDITION_SEVERITY_CONCEPT_ID) VALUES"
							+ "(?, ?, ?, ?)");

//			rsOMOP = stmtOMOP
//					.executeQuery("SELECT VISIT_OCCURRENCE_ID FROM VISIT_OCCURRENCE ORDER BY VISIT_OCCURRENCE_ID DESC");
//			if (rsOMOP.next()) {
//				visit_occurrence_id = rsOMOP.getLong("VISIT_OCCURRENCE_ID");
//			}

			rsExact = stmtExact.executeQuery("SELECT * FROM ENCOUNTER");
			while (rsExact.next()) {
				String member_id = rsExact.getString("MEMBER_ID");
				String NPI = rsExact.getString("PROVIDER_NPI");
				long person_id;
				Timestamp encounter_ts = rsExact
						.getTimestamp("ENCOUNTER_DATETIME");
				String clinic_type = rsExact.getString("CLINIC_TYPE");
				String place_of_service_value = rsExact
						.getString("ENCOUNTER_ID");
				String episode_id = rsExact.getString("EPISODE_ID");
				String encounter_note = rsExact.getString("SOAP_NOTE");

				// See if we already have this.
				rsOMOP = stmtOMOP
						.executeQuery("SELECT * FROM VISIT_OCCURRENCE WHERE PLACE_OF_SERVICE_SOURCE_VALUE='"
								+ place_of_service_value + "'");
				if (rsOMOP.next()) {
					// We have this entry.
					System.out.println("Visit/Encounter ("
							+ rsOMOP.getLong("VISIT_OCCURRENCE_ID")
							+ ") exits.");
					continue;
				}

				long place_of_service_concept_id;
				if (clinic_type.toLowerCase().contains("emergency")) {
					place_of_service_concept_id = emergency;
				} else if (clinic_type.toLowerCase().contains("outpatient")) {
					place_of_service_concept_id = outpatient_hospital;
				} else if (clinic_type.toLowerCase().contains("inpatient")) {
					place_of_service_concept_id = inpatient_hospital;
				} else if (clinic_type.toLowerCase().contains("pharmacy")) {
					place_of_service_concept_id = pharmacy;
				} else if (clinic_type.toLowerCase().contains("lab")) {
					place_of_service_concept_id = independent_lab;
				} else {
					place_of_service_concept_id = office;
				}

				long omop_episode_id = 0;
				boolean create_episode = false;
				if (episode_id != null && !episode_id.isEmpty()) {
					rsOMOP = stmtOMOP
							.executeQuery("SELECT EPISODE_OF_CARE_ID FROM EPISODE_OF_CARE WHERE EPISODE_SOURCE_VALUE='"
									+ episode_id + "'");
					if (rsOMOP.next()) {
						omop_episode_id = rsOMOP.getLong("EPISODE_OF_CARE_ID");
					} else {
//						rsOMOP = stmtOMOP
//								.executeQuery("SELECT EPISODE_OF_CARE_ID FROM EPISODE_OF_CARE ORDER BY EPISODE_OF_CARE_ID DESC");
//						if (rsOMOP.next()) {
//							omop_episode_id = rsOMOP
//									.getLong("EPISODE_OF_CARE_ID") + 1;
//						} else {
//							omop_episode_id = 1;
//						}
						create_episode = true;
					}
				}

				long care_site_id = 0;
				long provider_id = 0;
				rsOMOP = stmtOMOP
						.executeQuery("SELECT PROVIDER_ID, CARE_SITE_ID FROM PROVIDER WHERE NPI='"
								+ NPI + "'");
				if (rsOMOP.next()) {
					care_site_id = rsOMOP.getLong("CARE_SITE_ID");
					provider_id = rsOMOP.getLong("PROVIDER_ID");
				}

				rsOMOP = stmtOMOP
						.executeQuery("SELECT PERSON_ID FROM PERSON WHERE PERSON_SOURCE_VALUE='"
								+ member_id + "'");
				if (rsOMOP.next()) {
					person_id = rsOMOP.getLong("PERSON_ID");
					if (create_episode) {
						// Create Episode
//						pstmtOMOPEpisode.setLong(1, omop_episode_id);
						pstmtOMOPEpisode.setLong(1, person_id);
						pstmtOMOPEpisode.setString(2, episode_id);
						pstmtOMOPEpisode.executeUpdate();
						
						ResultSet tableKeys = pstmtOMOPEpisode.getGeneratedKeys();
						tableKeys.next();
						omop_episode_id = tableKeys.getInt(1);
					}
//					visit_occurrence_id++;
//					pstmtOMOPVisit.setLong(1, visit_occurrence_id);
					pstmtOMOPVisit.setLong(1, person_id);
					pstmtOMOPVisit.setTimestamp(2, encounter_ts);
					pstmtOMOPVisit.setTimestamp(3, encounter_ts);
					pstmtOMOPVisit.setLong(4, place_of_service_concept_id);
					if (care_site_id > 0) {
						pstmtOMOPVisit.setLong(5, care_site_id);
					} else {
						pstmtOMOPVisit.setNull(5, Types.NULL);
					}
					pstmtOMOPVisit.setString(6, place_of_service_value);

					pstmtOMOPVisit.executeUpdate();

					ResultSet tableKeys = pstmtOMOPVisit.getGeneratedKeys();
					tableKeys.next();
					visit_occurrence_id = tableKeys.getInt(1);

					System.out.println("Adding Encounter ("
							+ visit_occurrence_id + ") at "
							+ encounter_ts.toString());

					// insert f_visit_occurrence if episode id exists
					if (omop_episode_id > 0
							|| (encounter_note != null && !encounter_note
									.isEmpty())) {
						pstmtOMOPVisitF.setLong(1, visit_occurrence_id);
						if (omop_episode_id > 0)
							pstmtOMOPVisitF.setLong(2, omop_episode_id);
						else
							pstmtOMOPVisitF.setNull(2, Types.NULL);

						if (encounter_note != null && !encounter_note.isEmpty()) {
							pstmtOMOPVisitF.setString(3, encounter_note);
						} else {
							pstmtOMOPVisitF.setNull(3, Types.NULL);
						}

						pstmtOMOPVisitF.executeUpdate();
					}

					// There are some conditions in the encounter. Search and
					// add the condition.
					// Search CC column from encounter

					String CC = rsExact.getString("CC");
					if (CC != null && !CC.isEmpty()) {
						// We have a new condition here. Add it.
						long condition_occurrence_id = 0;
//						ResultSet rsIndex = stmtOMOP
//								.executeQuery("SELECT CONDITION_OCCURRENCE_ID FROM CONDITION_OCCURRENCE ORDER BY CONDITION_OCCURRENCE_ID DESC");
//						if (rsIndex.next()) {
//							condition_occurrence_id = rsIndex
//									.getLong("CONDITION_OCCURRENCE_ID");
//						}

						long condition_severity = 0;
						String keywords = CC.toLowerCase();
						if (keywords.contains("severe")
								|| keywords.contains("critical")) {
							condition_severity = severe;
						} else if (keywords.contains("moderate")) {
							condition_severity = moderate;
						} else if (keywords.contains("mild")) {
							condition_severity = mild;
						} else if (keywords.contains("fatal")) {
							condition_severity = fatal;
						}

//						long condition_concept_id = 0;
						List<Long> condition_concept_ids = new ArrayList<Long>();

						if (keywords.contains("weight")
								&& keywords.contains("loss")) {
							condition_concept_ids.add(new Long(4229881));
						}
						if (keywords.contains("amputation")
								&& keywords.contains("swelling")) {
							condition_concept_ids.add(new Long(4275722));
						}
						if (keywords.contains("stump")
								&& keywords.contains("pain")) {
							condition_concept_ids.add(new Long(4082797));
						}
						if ((keywords.contains("shortness")
								&& keywords.contains("breath")) 
								|| keywords.contains("sob")) {
							condition_concept_ids.add(new Long(312437));
						}
						if (keywords.contains("increased")
								&& keywords.contains("thirst")) {
							condition_concept_ids.add(new Long(4091956));
						}
						if (keywords.contains("frequent")
								&& keywords.contains("urination")) {
							condition_concept_ids.add(new Long(4012368));
						}
						if (keywords.contains("fever")) {
							condition_concept_ids.add(new Long(437663));
						}
						if (keywords.contains("cough")) {
							condition_concept_ids.add(new Long(254761));
						}
						if (keywords.contains("chest")
								&& keywords.contains("pain")) {
							condition_concept_ids.add(new Long(77670));
						}
						if (keywords.contains("foot")
								&& keywords.contains("pain")) {
							condition_concept_ids.add(new Long(4169905));
						}
						if (keywords.contains("pyelonephritis")) {
							condition_concept_ids.add(new Long(198199));
						}
						if (keywords.contains("myocardial")
								&& keywords.contains("infarction")) {
							condition_concept_ids.add(new Long(4329847));
						}
						if (keywords.contains("weak")
								&& keywords.contains("uniary")
								&& keywords.contains("stream")) {
							condition_concept_ids.add(new Long(195926));
						}
						if (keywords.contains("uniary")
								&& keywords.contains("frequency")) {
							condition_concept_ids.add(new Long(40519497));
						}
						if (keywords.contains("uniary")
								&& keywords.contains("dribbling")) {
							condition_concept_ids.add(new Long(200848));
						}
						if (keywords.contains("tingling")) {
							condition_concept_ids.add(new Long(4171909));
						}
						if (keywords.contains("paresthesia")
								|| keywords.contains("parethesia")) {
							condition_concept_ids.add(new Long(4236484));
						}
						if (keywords.contains("lethargy")) {
							condition_concept_ids.add(new Long(4061577));
						}
						if (keywords.contains("hyperesthesia")) {
							condition_concept_ids.add(new Long(4031840));
						}
						if (keywords.contains("discomfort")
								&& keywords.contains("abdominal")) {
							condition_concept_ids.add(new Long(40304436));
						} else if (keywords.contains("discomfort")) {
							condition_concept_ids.add(new Long(4090431));
						}
						if (keywords.contains("blurred")
								&& keywords.contains("vision")) {
							condition_concept_ids.add(new Long(44790749));
						}
						if (keywords.contains("back")
								&& keywords.contains("pain")) {
							condition_concept_ids.add(new Long(40303797));
						}
						if (keywords.contains("swellon")
								&& keywords.contains("ankle")) {
							condition_concept_ids.add(new Long(4144411));
						}
						if (keywords.contains("spell")
								&& keywords.contains("vertigo")) {
							condition_concept_ids.add(new Long(4198449));
						}
						if (keywords.contains("ring")
								&& keywords.contains("ear")) {
							condition_concept_ids.add(new Long(40305660));
						}
						if (keywords.contains("phantom")
								&& keywords.contains("limb")
								&& keywords.contains("pain")) {
							condition_concept_ids.add(new Long(40391537));
						}
						if (keywords.contains("headache")
								&& keywords.contains("morning")) {
							condition_concept_ids.add(new Long(4036953));
						}
						if (keywords.contains("lightheadedness")) {
							condition_concept_ids.add(new Long(4297376));
						}
						if (keywords.contains("dry")
								&& keywords.contains("mouth")) {
							condition_concept_ids.add(new Long(40304401));
						}
						if (keywords.contains("palpitation")) {
							condition_concept_ids.add(new Long(40303884));
						}
						if (keywords.contains("hemorrhagic")
								&& keywords.contains("stroke")) {
							condition_concept_ids.add(new Long(4217246));
						}
						if (keywords.contains("hysteria")) {
							condition_concept_ids.add(new Long(40615073));
						}
						if (keywords.contains("dyspnea")) {
							condition_concept_ids.add(new Long(40303859));
						}
						if (keywords.contains("chronic")
								&& keywords.contains("renal")
								&& keywords.contains("failure")) {
							condition_concept_ids.add(new Long(198185));
						}
						if (keywords.contains("acute")
								&& keywords.contains("renal")
								&& keywords.contains("failure")) {
							condition_concept_ids.add(new Long(40317620));
						}
						if (keywords.contains("weakness")) {
							condition_concept_ids.add(new Long(40305089));
						}
						if (keywords.contains("epigastric")
								&& keywords.contains("pain")) {
							condition_concept_ids.add(new Long(40304442));
						}
						if (keywords.contains("abdominal")
								&& keywords.contains("pressure")) {
							condition_concept_ids.add(new Long(4089147));
						}
						if (keywords.contains("lip")
								&& keywords.contains("ulcer")) {
							condition_concept_ids.add(new Long(4271702));
						}
						if (keywords.contains("elbow")
								&& keywords.contains("ulcer")) {
							condition_concept_ids.add(new Long(439410));
						}
						if (keywords.contains("bleed")
								&& keywords.contains("ulcer")) {
							condition_concept_ids.add(new Long(4208270));
						}
						if (keywords.contains("shoulder")
								&& keywords.contains("pain")) {
							condition_concept_ids.add(new Long(4166666));
						}
						if (keywords.contains("flushing")) {
							condition_concept_ids.add(new Long(318566));
						}
						if (keywords.contains("wheezing")) {
							condition_concept_ids.add(new Long(314754));
						}
						if (keywords.contains("vomiting")) {
							condition_concept_ids.add(new Long(40325777));
						}
						if (keywords.contains("appetite")
								&& keywords.contains("loss")) {
							condition_concept_ids.add(new Long(40303287));
						}
						if (keywords.contains("pelvic")
								&& keywords.contains("pain")) {
							condition_concept_ids.add(new Long(44801854));
						}
						if (keywords.contains("abdominal")
								&& keywords.contains("distension")) {
							condition_concept_ids.add(new Long(4218984));
						}
						if (keywords.contains("thoracic")
								&& keywords.contains("pain")) {
							condition_concept_ids.add(new Long(40404649));
						}
						if (keywords.contains("diarrhea")) {
							condition_concept_ids.add(new Long(196523));
						}
						if (keywords.contains("abnormal")
								&& keywords.contains("uterine")
								&& keywords.contains("leeding")) {
							condition_concept_ids.add(new Long(4208217));
						}
						if (keywords.contains("tiredness")) {
							condition_concept_ids.add(new Long(40502727));
						}
						if (keywords.contains("polyp")) {
							condition_concept_ids.add(new Long(4209223));
						}
						if (keywords.contains("breathlessness")) {
							condition_concept_ids.add(new Long(40325176));
						}
						if (keywords.contains("microcalcification")
								&& keywords.contains("breast")) {
							condition_concept_ids.add(new Long(40587848));
						}
						if (keywords.contains("nodule")
								&& keywords.contains("breast")) {
							condition_concept_ids.add(new Long(4126242));
						}
						if (keywords.contains("pruritus")) {
							condition_concept_ids.add(new Long(40319783));
						}
						if (keywords.contains("anorexia")) {
							condition_concept_ids.add(new Long(40388748));
						}
						if (keywords.contains("dysphagia")) {
							condition_concept_ids.add(new Long(31317));
						}
						if (keywords.contains("hematochezia")) {
							condition_concept_ids.add(new Long(443530));
						}
						if (keywords.contains("dark")
								&& keywords.contains("urine")) {
							condition_concept_ids.add(new Long(40304979));
						}

						// if it contains denial, we skip it as we don't know how to handle this
						if (keywords.contains("denial")) {
							condition_concept_ids.clear();
						}
						
						String condition_status = confirmed;

						if (condition_concept_ids.size() == 0) {
							// add to condition occurence table.
//							pstmtOMOPCondition1.setLong(1,
//									++condition_occurrence_id);
							pstmtOMOPCondition1.setLong(1, person_id);
							pstmtOMOPCondition1.setLong(2, 0);
							pstmtOMOPCondition1.setTimestamp(3, encounter_ts);
							pstmtOMOPCondition1.setNull(4, Types.NULL);
							pstmtOMOPCondition1.setLong(5, primary_condition);
							pstmtOMOPCondition1.setLong(6, provider_id);
							pstmtOMOPCondition1.setLong(7, visit_occurrence_id);
							pstmtOMOPCondition1.setString(8, place_of_service_value);
							pstmtOMOPCondition1.executeUpdate();

							ResultSet tableKeys1 = pstmtOMOPCondition1.getGeneratedKeys();
							tableKeys1.next();
							condition_occurrence_id = tableKeys1.getInt(1);

							// condition status - this is a required field in
							// the
							// FHIR
							// condition resource.
							// ExactData does not have statusinformation on
							// their
							// conditions
							// in encounter table. So, we will use confirmed for
							// all
							// conditions.
							pstmtOMOPConditionF1.setLong(1,
									condition_occurrence_id);
							pstmtOMOPConditionF1.setString(2, condition_status);
							pstmtOMOPConditionF1.setString(3, CC);
							pstmtOMOPConditionF1.setLong(4, condition_severity);
							pstmtOMOPConditionF1.executeUpdate();

						} else {
							for (int i = 0; i < condition_concept_ids.size(); i++) {
								// add to condition occurence table.
//								pstmtOMOPCondition2.setLong(1,
//										++condition_occurrence_id);
								pstmtOMOPCondition2.setLong(1, person_id);
								pstmtOMOPCondition2.setLong(2,
										condition_concept_ids.get(i));
								pstmtOMOPCondition2.setTimestamp(3,
										encounter_ts);
								pstmtOMOPCondition2.setNull(4, Types.NULL);
								pstmtOMOPCondition2.setLong(5, primary_condition);
								pstmtOMOPCondition2.setLong(6, provider_id);
								pstmtOMOPCondition2.setLong(7, visit_occurrence_id);
								pstmtOMOPCondition2.setString(8, place_of_service_value);
								pstmtOMOPCondition2.executeUpdate();

								ResultSet tableKeys2 = pstmtOMOPCondition2.getGeneratedKeys();
								tableKeys2.next();
								condition_occurrence_id = tableKeys2.getInt(1);

								pstmtOMOPConditionF2.setLong(1,
										condition_occurrence_id);
								pstmtOMOPConditionF2.setString(2,
										condition_status);
								pstmtOMOPConditionF2.setString(3, CC);
								pstmtOMOPConditionF2.setLong(4,
										condition_severity);
								pstmtOMOPConditionF2.executeUpdate();
							}
						}
					}

				} else {
					// No Person ID?? skip this...
					System.out
							.println("person_id not found for this encounter of member_id:"
									+ member_id);
					continue;
				}
			}

			stmtExact.close();
			pstmtOMOPEpisode.close();
			pstmtOMOPVisit.close();
			pstmtOMOPVisitF.close();
			pstmtOMOPCondition1.close();
			pstmtOMOPConditionF1.close();
			pstmtOMOPCondition2.close();
			pstmtOMOPConditionF2.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		} finally {
			try {
				connExact.close();
				connOMOP.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private void labs_map() {
		Statement stmtExact = null;
		Statement stmtOMOP = null;
		ResultSet rsExact = null;
		ResultSet rsOMOP = null;
		PreparedStatement pstmtOMOP = null;

		setup_database();

		try {
			stmtExact = connExact.createStatement();
			stmtOMOP = connOMOP.createStatement();

			long observation_id = 0;
//			rsOMOP = stmtOMOP
//					.executeQuery("SELECT OBSERVATION_ID FROM OBSERVATION ORDER BY OBSERVATION_ID DESC");
//			if (rsOMOP.next()) {
//				observation_id = rsOMOP.getLong("OBSERVATION_ID");
//			}

			pstmtOMOP = connOMOP
					.prepareStatement("INSERT INTO OBSERVATION"
							+ " (PERSON_ID, OBSERVATION_CONCEPT_ID, OBSERVATION_DATE, OBSERVATION_TIME, "
							+ "VALUE_AS_NUMBER, VALUE_AS_STRING, VALUE_AS_CONCEPT_ID, UNIT_CONCEPT_ID, RANGE_LOW, RANGE_HIGH, "
							+ "OBSERVATION_TYPE_CONCEPT_ID, ASSOCIATED_PROVIDER_ID, VISIT_OCCURRENCE_ID, OBSERVATION_SOURCE_VALUE, "
							+ "UNITS_SOURCE_VALUE) VALUES"
							+ " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);

			rsExact = stmtExact.executeQuery("SELECT * FROM LAB_RESULTS");
			while (rsExact.next()) {
				long person_id = 0;
				String member_id = rsExact.getString("MEMBER_ID");
				rsOMOP = stmtOMOP
						.executeQuery("SELECT PERSON_ID FROM PERSON WHERE PERSON_SOURCE_VALUE='"
								+ member_id + "'");
				if (rsOMOP.next()) {
					person_id = rsOMOP.getLong("PERSON_ID");
					long lab_concept_id = 0;
					rsOMOP = stmtOMOP
							.executeQuery("SELECT CONCEPT_ID FROM CONCEPT WHERE VOCABULARY_ID=6 AND CONCEPT_CODE='"
									+ rsExact.getString("RESULT_LOINC") + "'");
					if (rsOMOP.next()) {
						// Concept Code for LOINC found
						lab_concept_id = rsOMOP.getLong("CONCEPT_ID");
						Timestamp obs_date = rsExact
								.getTimestamp("DATE_RESULTED");
						SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
						String dfs = df.format(obs_date.getTime());
						String numeric_value = rsExact
								.getString("NUMERIC_RESULT");
						String text_value = rsExact
								.getString("RESULT_DESCRIPTION").replace("'", "''");

						String TEMP_SQL = "";
						if (text_value != null && !text_value.isEmpty()) {
							TEMP_SQL = "SELECT * FROM OBSERVATION WHERE PERSON_ID="
									+ person_id
									+ " AND OBSERVATION_CONCEPT_ID="
									+ lab_concept_id
									+ " AND VALUE_AS_STRING='"
									+ text_value
									+ "' AND DATE(OBSERVATION_DATE)='"
									+ dfs
									+ "'";
						} else if (numeric_value != null
								&& !numeric_value.isEmpty()) {

							TEMP_SQL = "SELECT * FROM OBSERVATION WHERE PERSON_ID="
									+ person_id
									+ " AND OBSERVATION_CONCEPT_ID="
									+ lab_concept_id
									+ " AND VALUE_AS_NUMBER="
									+ numeric_value
									+ " AND DATE(OBSERVATION_DATE)='"
									+ dfs
									+ "'";
						}

						if (TEMP_SQL != "") {
							rsOMOP = stmtOMOP.executeQuery(TEMP_SQL);

							if (rsOMOP.next()) {
								long obs_id = rsOMOP.getLong("OBSERVATION_ID");
								System.out.println("Lab for Member_ID:"
										+ member_id + " and observation_id = "
										+ obs_id + " exists. Skipping.");
								continue;
							}
						}

						long visit_occurrence_id = 0;
						rsOMOP = stmtOMOP
								.executeQuery("SELECT VISIT_OCCURRENCE_ID FROM VISIT_OCCURRENCE WHERE PLACE_OF_SERVICE_SOURCE_VALUE='"
										+ rsExact.getString("ENCOUNTER_ID")
										+ "'");
						if (rsOMOP.next()) {
							visit_occurrence_id = rsOMOP
									.getLong("VISIT_OCCURRENCE_ID");
						}

						// Now we are ready to add lab
//						pstmtOMOP.setLong(1, ++observation_id);
						pstmtOMOP.setLong(1, person_id);
						pstmtOMOP.setLong(2, lab_concept_id);
						pstmtOMOP.setDate(3, new Date(obs_date.getTime()));
						pstmtOMOP.setTimestamp(4, obs_date);
						if (numeric_value != null && !numeric_value.isEmpty()) {
							numeric_value = numeric_value.replace(",", "");
							pstmtOMOP.setDouble(5,
									Double.parseDouble(numeric_value));
							pstmtOMOP.setLong(11, observation_numeric_result);
						} else {
							pstmtOMOP.setNull(5, Types.NULL);
							pstmtOMOP.setLong(11, observation_text);
						}
						if (text_value != null && !text_value.isEmpty()) {
							pstmtOMOP.setString(6, text_value);
						} else {
							pstmtOMOP.setNull(6, Types.NULL);
						}

						pstmtOMOP.setNull(7, Types.NULL);

						// unit concept id.
						String unit = rsExact.getString("UNITS");
						long unit_concept_id = 0;
						if (unit.equals("mg/dL")) {
							unit_concept_id = Lab_mg_dL;
						} else if (unit.equals("%")) {
							unit_concept_id = Lab_percent;
						} else if (unit.equals("ng/mL")) {
							unit_concept_id = Lab_ng_mL;
						} else if (unit.equals("mm[Hg]")) {
							unit_concept_id = BP_unit_cid;
						} else if (unit.equals("meq/L")) {
							unit_concept_id = Lab_meq_L;
						} else if (unit.equals("U/mL")) {
							unit_concept_id = Lab_U_mL;
						} else if (unit.equals("mg/L")) {
							unit_concept_id = Lab_mg_L;
						} else if (unit.equals("mL/min")) {
							unit_concept_id = Lab_mL_min;
						} else if (unit.equals("{cells}/uL")) {
							unit_concept_id = Lab_cells_uL;
						} else if (unit.equals("10*6{cells}/uL")) {
							unit_concept_id = Lab_10_6_cells_uL;
						} else if (unit.equals("g/dL")) {
							unit_concept_id = Lab_g_dL;
						} else if (unit.equals("pg/mL")) {
							unit_concept_id = Lab_pg_mL;
						} else if (unit.equals("pg")) {
							unit_concept_id = Lab_pg;
						} else if (unit.equals("/uL")) {
							unit_concept_id = Lab_uL;
						}

						if (unit_concept_id == 0) {
							pstmtOMOP.setNull(8, Types.NULL);
						} else {
							pstmtOMOP.setLong(8, unit_concept_id);
						}

						pstmtOMOP.setNull(9, Types.NULL);
						pstmtOMOP.setNull(10, Types.NULL);

						rsOMOP = stmtOMOP
								.executeQuery("SELECT PROVIDER_ID FROM PROVIDER WHERE PROVIDER_SOURCE_VALUE='"
										+ rsExact.getString("PROVIDER_ID")
										+ "'");
						long provider_id = 0;
						if (rsOMOP.next()) {
							provider_id = rsOMOP.getLong("PROVIDER_ID");
						}
						if (provider_id == 0)
							pstmtOMOP.setNull(12, Types.NULL);
						else
							pstmtOMOP.setLong(12, provider_id);
						if (visit_occurrence_id == 0)
							pstmtOMOP.setNull(13, Types.NULL);
						else
							pstmtOMOP.setLong(13, visit_occurrence_id);

						pstmtOMOP.setString(14,
								rsExact.getString("RESULT_LOINC"));
						pstmtOMOP.setString(15, unit);

						pstmtOMOP.executeUpdate();

						ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
						tableKeys.next();
						observation_id = tableKeys.getInt(1);
					
						System.out.println("Adding labs (" + observation_id
								+ ")");
					} else {
						System.out.println("Failed to add Lab for Member_ID:"
								+ member_id
								+ " Failed to get concept ID for concept_code:"
								+ rsExact.getString("RESULT_LOINC"));
						continue;
					}
				} else {
					System.out.println("Failed to add Lab for Member_ID:"
							+ member_id + " [person_id] not found");
					continue;
				}
			}

			stmtExact.close();
			stmtOMOP.close();
			pstmtOMOP.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				connExact.close();
				connOMOP.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void vitals_map() {
		Statement stmtExact = null;
		Statement stmtOMOP = null;
		ResultSet rsExact = null;
		ResultSet rsOMOP = null;

		long observation_id = 0;
		setup_database();

		double height_for_bmi = 0.0, weight_for_bmi = 0.0;

		// Add Vitals.
		try {
			String SQL_STATEMENT_FROM = "SELECT * FROM VITAL_SIGN V, ENCOUNTER E WHERE V.ENCOUNTER_ID = E.ENCOUNTER_ID";
			stmtExact = connExact.createStatement();
			stmtOMOP = connOMOP.createStatement();
			PreparedStatement pstmtOMOP = connOMOP
					.prepareStatement("INSERT INTO OBSERVATION"
							+ " (PERSON_ID, OBSERVATION_CONCEPT_ID, OBSERVATION_DATE, OBSERVATION_TIME, VALUE_AS_NUMBER, VALUE_AS_STRING, VALUE_AS_CONCEPT_ID, UNIT_CONCEPT_ID, RANGE_LOW, RANGE_HIGH, OBSERVATION_TYPE_CONCEPT_ID, ASSOCIATED_PROVIDER_ID, OBSERVATION_SOURCE_VALUE, UNITS_SOURCE_VALUE, VISIT_OCCURRENCE_ID) VALUES"
							+ " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);

//			rsOMOP = stmtOMOP
//					.executeQuery("SELECT OBSERVATION_ID FROM OBSERVATION ORDER BY OBSERVATION_ID DESC");
//			if (rsOMOP.next()) {
//				observation_id = rsOMOP.getLong("OBSERVATION_ID");
//			}

			rsExact = stmtExact.executeQuery(SQL_STATEMENT_FROM);
			while (rsExact.next()) {
				String ExactMemberID = rsExact.getString("MEMBER_ID");

				// find visit_occurrence_id from OMOP database.
				long visit_occurrence_id = 0;
				rsOMOP = stmtOMOP
						.executeQuery("SELECT VISIT_OCCURRENCE_ID FROM VISIT_OCCURRENCE WHERE PLACE_OF_SERVICE_SOURCE_VALUE='"
								+ rsExact.getString("ENCOUNTER_ID") + "'");
				if (rsOMOP.next()) {
					visit_occurrence_id = rsOMOP.getLong("VISIT_OCCURRENCE_ID");
				}

				rsOMOP = stmtOMOP
						.executeQuery("SELECT PERSON_ID FROM PERSON WHERE PERSON_SOURCE_VALUE='"
								+ ExactMemberID + "'");
				if (rsOMOP.next()) {
					long person_id = rsOMOP.getLong("PERSON_ID");
					Timestamp obs_date = rsExact.getTimestamp("ENCOUNTER_DATE");
					SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
					String dfs = df.format(obs_date.getTime());
					SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss");
					String tfs = tf.format(obs_date.getTime());

					// System.out.println("MemberID:"+ExactMemberID+", Date:"+dfs+", Time:"+tfs);

					String NPI = rsExact.getString("PROVIDER_NPI");
					// Find provider_id.
					ResultSet rsCheck = stmtOMOP
							.executeQuery("SELECT PROVIDER_ID FROM PROVIDER WHERE NPI="
									+ NPI);
					long provider_id = 0;
					if (rsCheck.next()) {
						provider_id = rsCheck.getLong("PROVIDER_ID");
					}

					// Height ************************
					String height_value = rsExact.getString("HEIGHT");
					String height_unit = rsExact.getString("HEIGHT_UNITS");

					long height_unit_concept_id;
					String height_unit_string;
					if (height_unit.equalsIgnoreCase("in")) {
						height_unit_concept_id = height_in_cid;
						height_unit_string = "[in_us]";
						height_for_bmi = Double.parseDouble(height_value) * 0.025;
					} else {
						// cm
						height_unit_concept_id = height_cm_cid;
						height_unit_string = "cm";
						height_for_bmi = Double.parseDouble(height_value) / 100.0;
					}

					// See if we have this height already.
					rsCheck = stmtOMOP
							.executeQuery("SELECT OBSERVATION_ID, DATE(OBSERVATION_DATE) AS DATE, TIME(OBSERVATION_TIME) AS TIME  FROM OBSERVATION WHERE PERSON_ID="
									+ person_id
									+ " AND VALUE_AS_NUMBER="
									+ height_value
									+ " AND OBSERVATION_CONCEPT_ID="
									+ height_cid
									+ " AND DATE(OBSERVATION_DATE) = '"
									+ dfs
									+ "'"
									+ " AND TIME(OBSERVATION_TIME) = '"
									+ tfs + "'");
					if (rsCheck.next()) {
						System.out.println("Height ("
								+ rsCheck.getLong("OBSERVATION_ID") + ") at "
								+ rsCheck.getString("DATE") + " "
								+ rsCheck.getString("TIME") + " exists");
					} else {
						// Insert the height.
//						observation_id++;
//						pstmtOMOP.setLong(1, observation_id);
						pstmtOMOP.setLong(1, person_id);
						pstmtOMOP.setLong(2, height_cid);
						// pstmtOMOP.setDate(3, new
						// Date(df.parse(dfs).getTime()));
						// pstmtOMOP.setDate(4, new
						// Date(tf.parse(tfs).getTime()));
						pstmtOMOP.setDate(3, new Date(obs_date.getTime()));
						pstmtOMOP.setTimestamp(4, obs_date);
						pstmtOMOP
								.setDouble(5, Double.parseDouble(height_value));
						pstmtOMOP.setNull(6, Types.NULL);
						pstmtOMOP.setNull(7, Types.NULL);
						pstmtOMOP.setLong(8, height_unit_concept_id);
						pstmtOMOP.setNull(9, Types.NULL);
						pstmtOMOP.setNull(10, Types.NULL);
						pstmtOMOP.setLong(11, observation_numeric_result);
						if (provider_id != 0) {
							pstmtOMOP.setLong(12, provider_id);
						} else {
							pstmtOMOP.setNull(12, Types.NULL);
						}
						pstmtOMOP.setString(13, "8302-2");
						pstmtOMOP.setString(14, height_unit_string);
						if (visit_occurrence_id == 0) {
							pstmtOMOP.setNull(15, Types.NULL);
						} else {
							pstmtOMOP.setLong(15, visit_occurrence_id);
						}

						pstmtOMOP.executeUpdate();
						
						ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
						tableKeys.next();
						observation_id = tableKeys.getInt(1);

						System.out.println("Adding Height (" + observation_id
								+ ")");
					}

					// Weight ************************
					String weight_value = rsExact.getString("WEIGHT");
					String weight_unit = rsExact.getString("WEIGHT_UNITS");

					long weight_unit_concept_id;
					String weight_unit_string;
					if (weight_unit.equalsIgnoreCase("lbs")) {
						weight_unit_concept_id = weight_lb_cid;
						weight_unit_string = "[lb_us]";
						weight_for_bmi = Double.parseDouble(weight_value) * 0.45;
					} else {
						// cm
						weight_unit_concept_id = weight_kg_cid;
						weight_unit_string = "kg";
						weight_for_bmi = Double.parseDouble(weight_value);
					}

					// See if we have this weight already.
					rsCheck = stmtOMOP
							.executeQuery("SELECT OBSERVATION_ID, DATE(OBSERVATION_DATE) AS DATE, TIME(OBSERVATION_TIME) AS TIME  FROM OBSERVATION WHERE PERSON_ID="
									+ person_id
									+ " AND VALUE_AS_NUMBER="
									+ weight_value
									+ " AND OBSERVATION_CONCEPT_ID="
									+ weight_cid
									+ " AND DATE(OBSERVATION_DATE) = '"
									+ dfs
									+ "'"
									+ " AND TIME(OBSERVATION_TIME) = '"
									+ tfs + "'");
					if (rsCheck.next()) {
						System.out.println("Weight ("
								+ rsCheck.getLong("OBSERVATION_ID") + ") at "
								+ rsCheck.getString("DATE") + " "
								+ rsCheck.getString("TIME") + " exists");
					} else {
						// Insert the weight.
//						observation_id++;
//						pstmtOMOP.setLong(1, observation_id);
						pstmtOMOP.setLong(1, person_id);
						pstmtOMOP.setLong(2, weight_cid);
						pstmtOMOP.setDate(3, new Date(obs_date.getTime()));
						pstmtOMOP.setTimestamp(4, obs_date);
						pstmtOMOP
								.setDouble(5, Double.parseDouble(weight_value));
						pstmtOMOP.setNull(6, Types.NULL);
						pstmtOMOP.setNull(7, Types.NULL);
						pstmtOMOP.setLong(8, weight_unit_concept_id);
						pstmtOMOP.setNull(9, Types.NULL);
						pstmtOMOP.setNull(10, Types.NULL);
						pstmtOMOP.setLong(11, observation_numeric_result);
						if (provider_id != 0) {
							pstmtOMOP.setLong(12, provider_id);
						} else {
							pstmtOMOP.setNull(12, Types.NULL);
						}
						pstmtOMOP.setString(13, "3141-9");
						pstmtOMOP.setString(14, weight_unit_string);
						if (visit_occurrence_id == 0) {
							pstmtOMOP.setNull(15, Types.NULL);
						} else {
							pstmtOMOP.setLong(15, visit_occurrence_id);
						}

						pstmtOMOP.executeUpdate();
						
						ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
						tableKeys.next();
						observation_id = tableKeys.getInt(1);

						System.out.println("Adding Weight (" + observation_id
								+ ")");
					}

					// BMI **************************** (kg/m^2)
					// See if we have this BMI already.
					if (weight_for_bmi > 0.0 && height_for_bmi > 0.0) {
						double bmi = weight_for_bmi
								/ (height_for_bmi * height_for_bmi);
						rsCheck = stmtOMOP
								.executeQuery("SELECT OBSERVATION_ID, DATE(OBSERVATION_DATE) AS DATE, TIME(OBSERVATION_TIME) AS TIME  FROM OBSERVATION WHERE PERSON_ID="
										+ person_id
										+ " AND VALUE_AS_NUMBER="
										+ bmi
										+ " AND OBSERVATION_CONCEPT_ID="
										+ bmi_cid
										+ " AND DATE(OBSERVATION_DATE) = '"
										+ dfs
										+ "'"
										+ " AND TIME(OBSERVATION_TIME) = '"
										+ tfs + "'");
						if (rsCheck.next()) {
							System.out.println("BMI ("
									+ rsCheck.getLong("OBSERVATION_ID")
									+ ") at " + rsCheck.getString("DATE") + " "
									+ rsCheck.getString("TIME") + " exists");
						} else {
							// Insert the BMI.
//							observation_id++;
//							pstmtOMOP.setLong(1, observation_id);
							pstmtOMOP.setLong(1, person_id);
							pstmtOMOP.setLong(2, bmi_cid);
							pstmtOMOP.setDate(3, new Date(obs_date.getTime()));
							pstmtOMOP.setTimestamp(4, obs_date);
							pstmtOMOP.setDouble(5, bmi);
							pstmtOMOP.setNull(6, Types.NULL);
							pstmtOMOP.setNull(7, Types.NULL);
							pstmtOMOP.setLong(8, bmi_unit_cid);
							pstmtOMOP.setNull(9, Types.NULL);
							pstmtOMOP.setNull(10, Types.NULL);
							pstmtOMOP.setLong(11, observation_numeric_result);
							if (provider_id != 0) {
								pstmtOMOP.setLong(12, provider_id);
							} else {
								pstmtOMOP.setNull(12, Types.NULL);
							}
							pstmtOMOP.setString(13, "39156-5");
							pstmtOMOP.setString(14, "kg/m2");
							if (visit_occurrence_id == 0) {
								pstmtOMOP.setNull(15, Types.NULL);
							} else {
								pstmtOMOP.setLong(15, visit_occurrence_id);
							}

							pstmtOMOP.executeUpdate();
							
							ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
							tableKeys.next();
							observation_id = tableKeys.getInt(1);

							System.out.println("Adding BMI (" + observation_id
									+ ")");
						}
					}

					// Blood Pressure ********************************
					String systolicBP_value = rsExact.getString("SYSTOLICBP");
					String diastolicBP_value = rsExact.getString("DIASTOLICBP");

					// See if we have this BP already. We only check Systolic BP
					// because in most cases,
					// two values are meatured at the same time.
					rsCheck = stmtOMOP
							.executeQuery("SELECT OBSERVATION_ID, DATE(OBSERVATION_DATE) AS DATE, TIME(OBSERVATION_TIME) AS TIME  FROM OBSERVATION WHERE PERSON_ID="
									+ person_id
									+ " AND VALUE_AS_NUMBER="
									+ systolicBP_value
									+ " AND OBSERVATION_CONCEPT_ID="
									+ systolic_BP_cid
									+ " AND DATE(OBSERVATION_DATE) = '"
									+ dfs
									+ "'"
									+ " AND TIME(OBSERVATION_TIME) = '"
									+ tfs + "'");
					
					if (rsCheck.next()) {
						System.out.println("BP [SystolicBP] ("
								+ rsCheck.getLong("OBSERVATION_ID") + ") at "
								+ rsCheck.getString("DATE") + " "
								+ rsCheck.getString("TIME") + " exists");
					} else {
						// store observation_ids for grouped BP
						long systolic_ob_id;
						long diastolic_ob_id;
						
						// Insert the BP.
//						observation_id++;
//						systolic_ob_id = observation_id;
//						pstmtOMOP.setLong(1, observation_id);
						pstmtOMOP.setLong(1, person_id);
						pstmtOMOP.setLong(2, systolic_BP_cid);
						pstmtOMOP.setDate(3, new Date(obs_date.getTime()));
						pstmtOMOP.setTimestamp(4, obs_date);
						pstmtOMOP.setInt(5, Integer.parseInt(systolicBP_value));
						pstmtOMOP.setNull(6, Types.NULL);
						pstmtOMOP.setNull(7, Types.NULL);
						pstmtOMOP.setLong(8, BP_unit_cid);
						pstmtOMOP.setNull(9, Types.NULL);
						pstmtOMOP.setNull(10, Types.NULL);
						pstmtOMOP.setLong(11, observation_numeric_result);
						if (provider_id != 0) {
							pstmtOMOP.setLong(12, provider_id);
						} else {
							pstmtOMOP.setNull(12, Types.NULL);
						}
						pstmtOMOP.setString(13, "8480-6");
						pstmtOMOP.setString(14, "mm[Hg]");
						if (visit_occurrence_id == 0) {
							pstmtOMOP.setNull(15, Types.NULL);
						} else {
							pstmtOMOP.setLong(15, visit_occurrence_id);
						}

						pstmtOMOP.executeUpdate();
						ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
						tableKeys.next();
						observation_id = tableKeys.getInt(1);
						systolic_ob_id = observation_id;
						
						System.out.println("Adding SystolicBP ("
								+ observation_id + ")");

//						observation_id++;					
//						diastolic_ob_id = observation_id;
//						pstmtOMOP.setLong(1, observation_id);
						pstmtOMOP.setLong(1, person_id);
						pstmtOMOP.setLong(2, diastolic_BP_cid);
						pstmtOMOP.setDate(3, new Date(obs_date.getTime()));
						pstmtOMOP.setTimestamp(4, obs_date);
						pstmtOMOP
								.setInt(5, Integer.parseInt(diastolicBP_value));
						pstmtOMOP.setNull(6, Types.NULL);
						pstmtOMOP.setNull(7, Types.NULL);
						pstmtOMOP.setLong(8, BP_unit_cid);
						pstmtOMOP.setNull(9, Types.NULL);
						pstmtOMOP.setNull(10, Types.NULL);
						pstmtOMOP.setLong(11, observation_numeric_result);
						if (provider_id != 0) {
							pstmtOMOP.setLong(12, provider_id);
						} else {
							pstmtOMOP.setNull(12, Types.NULL);
						}
						pstmtOMOP.setString(13, "8462-4");
						pstmtOMOP.setString(14, "mm[Hg]");
						if (visit_occurrence_id == 0) {
							pstmtOMOP.setNull(15, Types.NULL);
						} else {
							pstmtOMOP.setLong(15, visit_occurrence_id);
						}

						pstmtOMOP.executeUpdate();
						ResultSet tableKeys1 = pstmtOMOP.getGeneratedKeys();
						tableKeys1.next();
						observation_id = tableKeys1.getInt(1);
						diastolic_ob_id = observation_id;
						
						System.out.println("Adding DiastolicBP ("
								+ observation_id + ")");

						// Add group BP.
//						observation_id++;
//						pstmtOMOP.setLong(1, observation_id);
						pstmtOMOP.setLong(1, person_id);
						pstmtOMOP.setLong(2, systolic_diastolic_BP_cid);
						pstmtOMOP.setDate(3, new Date(obs_date.getTime()));
						pstmtOMOP.setTimestamp(4, obs_date);
						pstmtOMOP.setNull(5, Types.NULL);
						pstmtOMOP.setString(6, "Blood pressure "+systolicBP_value+"/"+diastolicBP_value+" mmHg");
						pstmtOMOP.setNull(7, Types.NULL);
						pstmtOMOP.setLong(8, BP_unit_cid);
						pstmtOMOP.setNull(9, Types.NULL);
						pstmtOMOP.setNull(10, Types.NULL);
						pstmtOMOP.setLong(11, observation_text);
						if (provider_id != 0) {
							pstmtOMOP.setLong(12, provider_id);
						} else {
							pstmtOMOP.setNull(12, Types.NULL);
						}
						// We put source of these two BP values here. This is
						// component type. So, use COMP tag. Then put systolic and diastolic 
						// observation IDs with comma separation
						pstmtOMOP.setString(13, "COMP,"+systolic_ob_id+","+diastolic_ob_id);
						pstmtOMOP.setString(14, "mm[Hg]");
						if (visit_occurrence_id == 0) {
							pstmtOMOP.setNull(15, Types.NULL);
						} else {
							pstmtOMOP.setLong(15, visit_occurrence_id);
						}

						pstmtOMOP.executeUpdate();
						
						ResultSet tableKeys2 = pstmtOMOP.getGeneratedKeys();
						tableKeys2.next();
						observation_id = tableKeys2.getInt(1);
						
						System.out.println("Adding GroupBP ("
								+ observation_id + ")");

					}
					
					// Heart Rate (Pulse) ***************************
					String heartrate_value = rsExact.getString("PULSE");

					// See if we have this heart rate already.
					rsCheck = stmtOMOP
							.executeQuery("SELECT OBSERVATION_ID, DATE(OBSERVATION_DATE) AS DATE, TIME(OBSERVATION_TIME) AS TIME  FROM OBSERVATION WHERE PERSON_ID="
									+ person_id
									+ " AND VALUE_AS_NUMBER="
									+ heartrate_value
									+ " AND OBSERVATION_CONCEPT_ID="
									+ heart_rate_cid
									+ " AND DATE(OBSERVATION_DATE) = '"
									+ dfs
									+ "'"
									+ " AND TIME(OBSERVATION_TIME) = '"
									+ tfs + "'");
					if (rsCheck.next()) {
						System.out.println("Heart Rate ("
								+ rsCheck.getLong("OBSERVATION_ID") + ") at "
								+ rsCheck.getString("DATE") + " "
								+ rsCheck.getString("TIME") + " exists");
					} else {
						// Insert the heart rate.
//						observation_id++;
//						pstmtOMOP.setLong(1, observation_id);
						pstmtOMOP.setLong(1, person_id);
						pstmtOMOP.setLong(2, heart_rate_cid);
						pstmtOMOP.setDate(3, new Date(obs_date.getTime()));
						pstmtOMOP.setTimestamp(4, obs_date);
						pstmtOMOP.setInt(5, Integer.parseInt(heartrate_value));
						pstmtOMOP.setNull(6, Types.NULL);
						pstmtOMOP.setNull(7, Types.NULL);
						pstmtOMOP.setLong(8, per_min_cid);
						pstmtOMOP.setNull(9, Types.NULL);
						pstmtOMOP.setNull(10, Types.NULL);
						pstmtOMOP.setLong(11, observation_numeric_result);
						if (provider_id != 0) {
							pstmtOMOP.setLong(12, provider_id);
						} else {
							pstmtOMOP.setNull(12, Types.NULL);
						}
						pstmtOMOP.setString(13, "8867-4");
						pstmtOMOP.setString(14, "{beats}/min");
						if (visit_occurrence_id == 0) {
							pstmtOMOP.setNull(15, Types.NULL);
						} else {
							pstmtOMOP.setLong(15, visit_occurrence_id);
						}

						pstmtOMOP.executeUpdate();

						ResultSet tableKeys3 = pstmtOMOP.getGeneratedKeys();
						tableKeys3.next();
						observation_id = tableKeys3.getInt(1);

						System.out.println("Adding Heart Rate ("
								+ observation_id + ")");
					}

					// Respiration Rate *********************************
					String respiration_value = rsExact.getString("RESPIRATION");

					// See if we have this respiration rate already.
					rsCheck = stmtOMOP
							.executeQuery("SELECT OBSERVATION_ID, DATE(OBSERVATION_DATE) AS DATE, TIME(OBSERVATION_TIME) AS TIME  FROM OBSERVATION WHERE PERSON_ID="
									+ person_id
									+ " AND VALUE_AS_NUMBER="
									+ respiration_value
									+ " AND OBSERVATION_CONCEPT_ID="
									+ respiration_rate_cid
									+ " AND DATE(OBSERVATION_DATE) = '"
									+ dfs
									+ "'"
									+ " AND TIME(OBSERVATION_TIME) = '"
									+ tfs + "'");
					if (rsCheck.next()) {
						System.out.println("Respiration Rate ("
								+ rsCheck.getLong("OBSERVATION_ID") + ") at "
								+ rsCheck.getString("DATE") + " "
								+ rsCheck.getString("TIME") + " exists");
					} else {
						// Insert the respiration rate.
//						observation_id++;
//						pstmtOMOP.setLong(1, observation_id);
						pstmtOMOP.setLong(1, person_id);
						pstmtOMOP.setLong(2, respiration_rate_cid);
						pstmtOMOP.setDate(3, new Date(obs_date.getTime()));
						pstmtOMOP.setTimestamp(4, obs_date);
						pstmtOMOP
								.setInt(5, Integer.parseInt(respiration_value));
						pstmtOMOP.setNull(6, Types.NULL);
						pstmtOMOP.setNull(7, Types.NULL);
						pstmtOMOP.setLong(8, per_min_cid);
						pstmtOMOP.setNull(9, Types.NULL);
						pstmtOMOP.setNull(10, Types.NULL);
						pstmtOMOP.setLong(11, observation_numeric_result);
						if (provider_id != 0) {
							pstmtOMOP.setLong(12, provider_id);
						} else {
							pstmtOMOP.setNull(12, Types.NULL);
						}
						pstmtOMOP.setString(13, "9279-1");
						pstmtOMOP.setString(14, "{breaths}/min");
						if (visit_occurrence_id == 0) {
							pstmtOMOP.setNull(15, Types.NULL);
						} else {
							pstmtOMOP.setLong(15, visit_occurrence_id);
						}

						pstmtOMOP.executeUpdate();
						
						ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
						tableKeys.next();
						observation_id = tableKeys.getInt(1);
						
						System.out.println("Adding Respiration Rate ("
								+ observation_id + ")");
					}

					// Temperature ************************************
					String temperature_value = rsExact.getString("TEMPERATURE");
					String temperature_unit = rsExact
							.getString("TEMPERATURE_UNITS");

					long temperature_unit_concept_id;
					String temperature_unit_string;
					if (temperature_unit.equalsIgnoreCase("F")) {
						temperature_unit_concept_id = temp_F_cid;
						temperature_unit_string = "[degF]";
					} else {
						// C
						temperature_unit_concept_id = temp_C_cid;
						temperature_unit_string = "[degC]";
					}

					// See if we have this temperature already.
					rsCheck = stmtOMOP
							.executeQuery("SELECT OBSERVATION_ID, DATE(OBSERVATION_DATE) AS DATE, TIME(OBSERVATION_TIME) AS TIME  FROM OBSERVATION WHERE PERSON_ID="
									+ person_id
									+ " AND VALUE_AS_NUMBER="
									+ temperature_value
									+ " AND OBSERVATION_CONCEPT_ID="
									+ temp_cid
									+ " AND DATE(OBSERVATION_DATE) = '"
									+ dfs
									+ "'"
									+ " AND TIME(OBSERVATION_TIME) = '"
									+ tfs + "'");
					if (rsCheck.next()) {
						System.out.println("Temperature ("
								+ rsCheck.getLong("OBSERVATION_ID") + ") at "
								+ rsCheck.getString("DATE") + " "
								+ rsCheck.getString("TIME") + " exists");
					} else {
						// Insert the temperature.
//						observation_id++;
//						pstmtOMOP.setLong(1, observation_id);
						pstmtOMOP.setLong(1, person_id);
						pstmtOMOP.setLong(2, temp_cid);
						pstmtOMOP.setDate(3, new Date(obs_date.getTime()));
						pstmtOMOP.setTimestamp(4, obs_date);
						pstmtOMOP.setDouble(5,
								Double.parseDouble(temperature_value));
						pstmtOMOP.setNull(6, Types.NULL);
						pstmtOMOP.setNull(7, Types.NULL);
						pstmtOMOP.setLong(8, temperature_unit_concept_id);
						pstmtOMOP.setNull(9, Types.NULL);
						pstmtOMOP.setNull(10, Types.NULL);
						pstmtOMOP.setLong(11, observation_numeric_result);
						if (provider_id != 0) {
							pstmtOMOP.setLong(12, provider_id);
						} else {
							pstmtOMOP.setNull(12, Types.NULL);
						}
						pstmtOMOP.setString(13, "8310-5");
						pstmtOMOP.setString(14, temperature_unit_string);
						if (visit_occurrence_id == 0) {
							pstmtOMOP.setNull(15, Types.NULL);
						} else {
							pstmtOMOP.setLong(15, visit_occurrence_id);
						}

						pstmtOMOP.executeUpdate();
						ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
						tableKeys.next();
						observation_id = tableKeys.getInt(1);
						
						System.out.println("Adding Temperature ("
								+ observation_id + ")");
					}

				} else {
					// No Person ID?? skip this...
					System.out
							.println("PersonID not found for this vital of member_id:"
									+ ExactMemberID);
					continue;
				}

			}
			stmtExact.close();
			stmtOMOP.close();
			pstmtOMOP.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				connExact.close();
				connOMOP.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	private void problem_map() {
		Statement stmtExact = null;
		Statement stmtOMOP = null;
		ResultSet rsExact = null;
		ResultSet rsOMOP = null;

		setup_database();

		String SQL_STATEMENT_FROM = "SELECT * FROM PROBLEM";
//		String SQL_STATEMENT_C_O_ID = "SELECT CONDITION_OCCURRENCE_ID FROM CONDITION_OCCURRENCE ORDER BY CONDITION_OCCURRENCE_ID DESC";

		String SQL_STATEMENT_TO_CONDITION_OCCUR = "INSERT INTO CONDITION_OCCURRENCE"
				+ " (PERSON_ID, CONDITION_CONCEPT_ID, CONDITION_START_DATE, CONDITION_END_DATE, CONDITION_TYPE_CONCEPT_ID, CONDITION_SOURCE_VALUE) VALUES"
				+ " (?,?,?,?,?,?)";
		String SQL_STATEMENT_ICD9toSCT_MAPPING = "SELECT ICD_CODE, ICD_NAME, SNOMED_CID, SNOMED_FSN FROM ICD9toSCT WHERE";

		long condition_occurrence_id = 0;
		try {
			stmtOMOP = connOMOP.createStatement();
			stmtExact = connExact.createStatement();

			PreparedStatement pstmtOMOP = connOMOP
					.prepareStatement(SQL_STATEMENT_TO_CONDITION_OCCUR, Statement.RETURN_GENERATED_KEYS);
			PreparedStatement pstmtOMOPF = connOMOP
					.prepareStatement("INSERT INTO F_CONDITION_OCCURRENCE (CONDITION_OCCURRENCE_ID, FHIR_CONDITION_DISPLAY) VALUES (?, ?)");

//			rsOMOP = stmtOMOP.executeQuery(SQL_STATEMENT_C_O_ID);
//			if (rsOMOP.next()) {
//				condition_occurrence_id = rsOMOP
//						.getLong("CONDITION_OCCURRENCE_ID");
//			}
//			rsOMOP.close();

			rsExact = stmtExact.executeQuery(SQL_STATEMENT_FROM);
			while (rsExact.next()) {
				// Get MEMBER_ID to get person_id.
				long person_id = 0;
				String member_id = rsExact.getString("MEMBER_ID");
				String prob_descript = rsExact.getString("Problem_Description");
				String SQL_STATEMENT_GET_PID = "SELECT PERSON_ID FROM PERSON WHERE PERSON_SOURCE_VALUE='"
						+ member_id + "'";
				rsOMOP = stmtOMOP.executeQuery(SQL_STATEMENT_GET_PID);
				if (rsOMOP.next()) {
					person_id = rsOMOP.getLong("PERSON_ID");

					// Map the ICD code to SNOMED-CT
					String ICDcode = rsExact.getString("PROBLEM_CODE");
					String SQL_STATEMENT_MAPPING = SQL_STATEMENT_ICD9toSCT_MAPPING
							+ " ICD_CODE='" + ICDcode + "' AND";
					String searchWords[] = prob_descript.trim()
							.replaceAll(" +", " ").split(" ");
					String searchPattern = "";
					for (int i = 0; i < searchWords.length; i++) {
						if (i == searchWords.length - 1) {
							searchPattern = searchPattern
									+ " SNOMED_FSN like '%" + searchWords[i]
									+ "%'";
						} else {
							searchPattern = searchPattern
									+ " SNOMED_FSN like '%" + searchWords[i]
									+ "%' AND";
						}
					}
					SQL_STATEMENT_MAPPING = SQL_STATEMENT_MAPPING + "("
							+ searchPattern + ") OR ";

					searchPattern = "";
					for (int i = 0; i < searchWords.length; i++) {
						if (i == searchWords.length - 1) {
							searchPattern = searchPattern + " ICD_NAME like '%"
									+ searchWords[i] + "%'";
						} else {
							searchPattern = searchPattern + " ICD_NAME like '%"
									+ searchWords[i] + "%' AND";
						}
					}
					SQL_STATEMENT_MAPPING = SQL_STATEMENT_MAPPING + "("
							+ searchPattern + ")";

					ResultSet rsMap = stmtOMOP
							.executeQuery(SQL_STATEMENT_MAPPING);
					String SCTcode = "";
					if (rsMap.next()) {
						// We may have multiple entries. Since we can't really
						// match, we just choose the first one.
						SCTcode = rsMap.getString("SNOMED_CID");
					} else {
						SQL_STATEMENT_MAPPING = SQL_STATEMENT_ICD9toSCT_MAPPING
								+ " ICD_CODE='" + ICDcode + "'";
						rsMap = stmtOMOP.executeQuery(SQL_STATEMENT_MAPPING);
						if (rsMap.next()) {
							SCTcode = rsMap.getString("SNOMED_CID");
						}
					}

					if (SCTcode == "") {
						// We have condition but no mapping found.
						System.out.println("Adding Condition, " + prob_descript
								+ ", for member_id, " + member_id
								+ ", is skipped becaused no mapping for "
								+ ICDcode);
						System.out.println(SQL_STATEMENT_MAPPING);

						rsMap.close();
						continue;
					} else {
						String SQL_STATEMENT_CONCEPT_ID = "SELECT CONCEPT_ID FROM CONCEPT WHERE CONCEPT_CODE = "
								+ SCTcode;
						ResultSet rsConcept = stmtOMOP
								.executeQuery(SQL_STATEMENT_CONCEPT_ID);
						long concept_id = 0;
						if (rsConcept.next()) {
							concept_id = rsConcept.getLong("CONCEPT_ID");

							// start date
							Date startDate = rsExact.getDate("ONSET_DATE");
							// Now, if we have an entry with the same onset
							// date,
							// same concept_id, and same person_id then
							// we don't need to enter this. It's duplicate.
							SimpleDateFormat df = new SimpleDateFormat(
									"yyyy-MM-dd");
							String stringSD = df.format(startDate);
							rsOMOP = stmtOMOP
									.executeQuery("SELECT COUNT(*) AS C FROM CONDITION_OCCURRENCE WHERE"
											+ " PERSON_ID="
											+ person_id
											+ " AND CONDITION_CONCEPT_ID="
											+ concept_id
											+ " AND DATE(CONDITION_START_DATE)='"
											+ stringSD + "'");
							if (rsOMOP.next()) {
								if (rsOMOP.getInt("C") > 0) {
									// Exists, skip
									System.out.println("Condition [cid:"
											+ concept_id + ", startdate:"
											+ stringSD + ", personid:"
											+ person_id
											+ "]exists. Skipping...");
									rsOMOP.close();
									continue;
								}
							}
							rsOMOP.close();

							// Now we have everything. Create a condition
							// occurrence entry.
//							condition_occurrence_id++;
//							pstmtOMOP.setLong(1, condition_occurrence_id);
							pstmtOMOP.setLong(1, person_id);
							pstmtOMOP.setLong(2, concept_id);

							pstmtOMOP.setDate(3, startDate);

							try {
								Date endDate = rsExact
										.getDate("RESOLUTION_DATE");
								if (endDate.after(startDate)) {
									pstmtOMOP.setDate(4, endDate);
								}
							} catch (SQLException e) {
								pstmtOMOP.setDate(4, null);
							}

							pstmtOMOP.setLong(5, EHR_prob_list_entry);
							pstmtOMOP.setString(6, ICDcode);

							pstmtOMOP.executeUpdate();
							ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
							tableKeys.next();
							condition_occurrence_id = tableKeys.getInt(1);
							
							pstmtOMOPF.setLong(1, condition_occurrence_id);
							pstmtOMOPF.setString(2, prob_descript);
							pstmtOMOPF.executeUpdate();

							System.out.println("Condition("
									+ condition_occurrence_id + ") added.");

						} else {
							// We found concept code, but our concept table does
							// not have this code.
							System.out
									.println("Adding Condition, "
											+ prob_descript
											+ ", for member_id, "
											+ member_id
											+ ", is skipped becaused no concept code in concept table SNOMED code= "
											+ SCTcode);
							rsConcept.close();
							continue;
						}
						rsConcept.close();
					}
					rsMap.close();
				} else {
					// We have condition but no matching person found.
					System.out
							.println("Adding Condition, "
									+ prob_descript
									+ ", for member_id, "
									+ member_id
									+ ", is skipped becaused no matching person_id found.");
					rsOMOP.close();
					continue;
				}
				rsOMOP.close();
			}
			rsExact.close();
			stmtExact.close();
			stmtOMOP.close();
			pstmtOMOP.close();
			pstmtOMOPF.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				connExact.close();
				connOMOP.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private void enrollment_map() {
		Statement stmtExact = null;
		Statement stmtOMOP = null;
		ResultSet rsExact = null;
		ResultSet rsOMOP = null;

		setup_database();

		// Person Information.
//		String SQL_STATEMENT_FROM = "SELECT * FROM ENROLLMENT";
		String SQL_STATEMENT_TO_PERSON = "INSERT INTO PERSON"
				+ " (GENDER_CONCEPT_ID, YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH, RACE_CONCEPT_ID, LOCATION_ID, PERSON_SOURCE_VALUE, GENDER_SOURCE_VALUE, RACE_SOURCE_VALUE, ETHNICITY_SOURCE_VALUE) VALUES "
				+ " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		String SQL_STATEMENT_TO_F_PERSON = "INSERT INTO F_PERSON"
				+ " (PERSON_ID, FAMILY_NAME, GIVEN1_NAME, GIVEN2_NAME, PREFIX_NAME, SSN, MARITALSTATUS_CONCEPT_ID) VALUES "
				+ " (?, ?, ?, ?, ?, ?, ?)";
//		String SQL_STATEMENT_PID = "SELECT PERSON_ID FROM PERSON ORDER BY PERSON_ID DESC";
		String SQL_STATEMENT_LOCATION = "INSERT INTO LOCATION"
				+ " (ADDRESS_1, ADDRESS_2, CITY, STATE, ZIP, LOCATION_SOURCE_VALUE) VALUES "
				+ " (? ,?, ?, ?, ?, ?)";
		String SQL_STATEMENT_F_LOCATION = "INSERT INTO F_LOCATION (LOCATION_ID) VALUES (?)";
//		String SQL_STATEMENT_LOCATIONID = "SELECT LOCATION_ID FROM LOCATION ORDER BY LOCATION_ID DESC";

		long person_id = 0;
		long location_id = 0;
		try {
			stmtOMOP = connOMOP.createStatement();
//			rsOMOP = stmtOMOP.executeQuery(SQL_STATEMENT_PID);
//			if (rsOMOP.next()) {
//				person_id = rsOMOP.getLong("PERSON_ID");
//			}

//			rsOMOP = stmtOMOP.executeQuery(SQL_STATEMENT_LOCATIONID);
//			if (rsOMOP.next()) {
//				location_id = rsOMOP.getLong("LOCATION_ID");
//			}

			PreparedStatement pstmt_person = connOMOP
					.prepareStatement(SQL_STATEMENT_TO_PERSON, Statement.RETURN_GENERATED_KEYS);
			PreparedStatement pstmt_f_person = connOMOP
					.prepareStatement(SQL_STATEMENT_TO_F_PERSON);
			PreparedStatement pstmt_location = connOMOP
					.prepareStatement(SQL_STATEMENT_LOCATION, Statement.RETURN_GENERATED_KEYS);
			PreparedStatement pstmt_f_location = connOMOP
					.prepareStatement(SQL_STATEMENT_F_LOCATION);

			stmtExact = connExact.createStatement();
			rsExact = stmtExact.executeQuery("SELECT * FROM ENROLLMENT");
			while (rsExact.next()) {
				// Check if this entry already exists.
				String ExactMemberID = rsExact.getString("Member_ID");

				String SQL_EXIST_CHECK_STATEMENT = "SELECT * FROM PERSON WHERE PERSON_SOURCE_VALUE='"
						+ ExactMemberID + "'";
				rsOMOP = stmtOMOP.executeQuery(SQL_EXIST_CHECK_STATEMENT);
				if (rsOMOP.next()) {
					System.out.println("New Person ID (" + (person_id + 1)
								+ ") exists already.. skipped");
					continue;
				}

//				person_id += 1;
				// location_id += 1;

//				pstmt_person.setLong(1, person_id);
				if (rsExact.getString("Gender").equalsIgnoreCase("male")) {
					pstmt_person.setLong(1, male_gender);
				} else {
					pstmt_person.setLong(1, female_gender);
				}
				String DOB = rsExact.getString("DOB");
				String DOBs[] = DOB.split("\\-");
				pstmt_person.setInt(2, Integer.parseInt(DOBs[0]));
				pstmt_person.setInt(3, Integer.parseInt(DOBs[1]));
				pstmt_person.setInt(4, Integer.parseInt(DOBs[2]));
				if (rsExact.getString("Race").equalsIgnoreCase("White")) {
					pstmt_person.setLong(5, white);
				} else if (rsExact.getString("Race").equalsIgnoreCase("Black")) {
					pstmt_person.setLong(5, black);
				} else if (rsExact.getString("Race").equalsIgnoreCase(
						"Native American")) {
					pstmt_person.setLong(5, amrican_indian_alaska_native);
				} else if (rsExact.getString("Race").equalsIgnoreCase("Asian")) {
					pstmt_person.setLong(5, asian);
				} else if (rsExact.getString("Race").equalsIgnoreCase(
						"Asian Indian")) {
					pstmt_person.setLong(5, asian);
				}

				// see if we have this address.
				String address1 = rsExact.getString("Address_Line_1");
				String address2 = rsExact.getString("Address_Line_2");
				String city = rsExact.getString("City");
				String state = rsExact.getString("State");
				String zip = rsExact.getString("Zip_Code");

				long actual_location_id;
				rsOMOP = stmtOMOP
						.executeQuery("SELECT LOCATION_ID FROM LOCATION WHERE "
								+ "ADDRESS_1='" + address1 + "' AND "
								+ "ADDRESS_2='" + address2 + "' AND "
								+ "CITY='" + city + "' AND " + "STATE='"
								+ state + "' AND " + "ZIP='" + zip + "'");
				if (rsOMOP.next()) {
					actual_location_id = rsOMOP.getLong("LOCATION_ID");
				} else {
//					location_id += 1;
//					actual_location_id = location_id;
//					pstmt_location.setLong(1, location_id);
					pstmt_location.setString(1, address1);
					pstmt_location.setString(2, address2);
					pstmt_location.setString(3, city);
					pstmt_location.setString(4, state);
					pstmt_location.setString(5, zip);
					pstmt_location.setString(6, ExactMemberID);
					
					pstmt_location.executeUpdate();
					ResultSet tableKeys = pstmt_location.getGeneratedKeys();
					tableKeys.next();
					location_id = tableKeys.getInt(1);
					
					pstmt_f_location.setLong(1, location_id);
					pstmt_f_location.executeUpdate();
					
					actual_location_id = location_id;
				}

				pstmt_person.setLong(6, actual_location_id);
				pstmt_person.setString(7, ExactMemberID);
				pstmt_person.setString(8, ExactMemberID);
				pstmt_person.setString(9, ExactMemberID);
				pstmt_person.setString(10, ExactMemberID);
				pstmt_person.executeUpdate();
				
				ResultSet tableKeys = pstmt_person.getGeneratedKeys();
				tableKeys.next();
				person_id = tableKeys.getInt(1);

				pstmt_f_person.setLong(1, person_id);
				pstmt_f_person.setString(2, rsExact.getString("Name_Last"));
				pstmt_f_person.setString(3, rsExact.getString("Name_First"));
				pstmt_f_person.setString(4, rsExact.getString("MI"));
				pstmt_f_person.setString(5, rsExact.getString("Title"));
				pstmt_f_person.setString(6, rsExact.getString("Member_SSN"));

				if (rsExact.getString("Marital_Status").equalsIgnoreCase(
						"Single")) {
					pstmt_f_person.setLong(7, single);
				} else if (rsExact.getString("Marital_Status")
						.equalsIgnoreCase("Married")) {
					pstmt_f_person.setLong(7, married);
				} else if (rsExact.getString("Marital_Status")
						.equalsIgnoreCase("Widowed")) {
					pstmt_f_person.setLong(7, widowed);
				} else if (rsExact.getString("Marital_Status")
						.equalsIgnoreCase("Married")) {
					pstmt_f_person.setLong(7, married);
				} else if (rsExact.getString("Marital_Status")
						.equalsIgnoreCase("Divorced")) {
					pstmt_f_person.setLong(7, divorced);
				} else if (rsExact.getString("Marital_Status")
						.equalsIgnoreCase("Legally Separated")) {
					pstmt_f_person.setLong(7, separated);
				}
				pstmt_f_person.executeUpdate();

				System.out.println(person_id + " are written to FHIR OMOP");
			}

			rsExact.close();
			rsOMOP.close();
			stmtExact.close();
			stmtOMOP.close();
			pstmt_location.clearParameters();
			pstmt_location.close();
			pstmt_f_location.clearParameters();
			pstmt_f_location.close();
			pstmt_person.clearParameters();
			pstmt_person.close();
			pstmt_f_person.clearParameters();
			pstmt_f_person.close();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				connExact.close();
				connOMOP.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	static long prescription_written_code = 38000177;
	static long prescription_dispensed_code = 38000175;

	private void medication_orders_map() {
		Statement stmtExact = null;
		Statement stmtOMOP = null;
		Statement stmtRxNORM = null;
		ResultSet rsExact = null;
		ResultSet rsOMOP = null;
		ResultSet rsRxNORM = null;

		PreparedStatement pstmtOMOP = null;
		PreparedStatement pstmtOMOPF = null;

		setup_database();

		long drug_exposure_id = 0;
		try {
			stmtExact = connExact.createStatement();
			stmtOMOP = connOMOP.createStatement();

			// Find next drug_exposure_id.
//			rsOMOP = stmtOMOP
//					.executeQuery("SELECT DRUG_EXPOSURE_ID FROM DRUG_EXPOSURE ORDER BY DRUG_EXPOSURE_ID DESC");
//			if (rsOMOP.next()) {
//				drug_exposure_id = rsOMOP.getLong("DRUG_EXPOSURE_ID");
//			}

			pstmtOMOP = connOMOP
					.prepareStatement("INSERT INTO DRUG_EXPOSURE (PERSON_ID, "
							+ "DRUG_CONCEPT_ID, DRUG_EXPOSURE_START_DATE, DRUG_EXPOSURE_END_DATE, DRUG_TYPE_CONCEPT_ID, "
							+ "STOP_REASON, REFILLS, QUANTITY, DAYS_SUPPLY, SIG, PRESCRIBING_PROVIDER_ID, VISIT_OCCURRENCE_ID, "
							+ "RELEVANT_CONDITION_CONCEPT_ID, DRUG_SOURCE_VALUE) VALUES "
							+ "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);

			pstmtOMOPF = connOMOP
					.prepareStatement("INSERT INTO F_DRUG_EXPOSURE (DRUG_EXPOSURE_ID, DOSE, UNIT) VALUES "
							+ "(?,?,?)");

			rsExact = stmtExact.executeQuery("SELECT * FROM MEDICATION_ORDERS"); // MedicationPrescription
			while (rsExact.next()) {
				String member_id = rsExact.getString("MEMBER_ID");
				rsOMOP = stmtOMOP
						.executeQuery("SELECT PERSON_ID FROM PERSON WHERE PERSON_SOURCE_VALUE='"
								+ member_id + "'");
				long person_id = 0;
				if (rsOMOP.next()) {
					person_id = rsOMOP.getLong("PERSON_ID");
				} else {
					System.out
							.println("Adding MedicationPrescription failed - couldn't find person_id");
					continue;
				}

				Timestamp drug_order_ts = rsExact.getTimestamp("ORDER_DATE");
				String NDC_code = rsExact.getString("DRUG_NDC");
				if (NDC_code.equals("67544-134-30")) {
					NDC_code = "0143-1268-01";
				}

				// Check if we have this entry already.
				rsOMOP = stmtOMOP
						.executeQuery("SELECT DRUG_EXPOSURE_ID FROM DRUG_EXPOSURE WHERE"
								+ " PERSON_ID="
								+ person_id
								+ " AND TIMESTAMP(DRUG_EXPOSURE_START_DATE)='"
								+ new Timestamp(drug_order_ts.getTime())
								+ "'"
								+ " AND DRUG_SOURCE_VALUE='"
								+ NDC_code
								+ "'"
								+ " AND DRUG_TYPE_CONCEPT_ID="
								+ prescription_written_code); // 38000177 is
																// prescription
																// written.
				if (rsOMOP.next()) {
					System.out.println("MedicationPrescription exists ["
							+ rsOMOP.getLong("DRUG_EXPOSURE_ID") + "]");
					continue;
				}

				long drug_concept_id = 0;
				// Get NDC code mapped to RxNORM.
				if (NDC_code.equals("54569-5133-0")) {
					drug_concept_id = 1341927;
				} else if (NDC_code.equals("54868-6054-0")) {// insulin aspart
					drug_concept_id = 1567198;
				} else if (NDC_code.equals("0083-4000-41")) {// valsartan
					drug_concept_id = 1308848;
				} else if (NDC_code.equals("0005-3122-23")) {
					drug_concept_id = 19081499;
				} else if (NDC_code.equals("50580-112-21")) {
					drug_concept_id = 1125393;
				} else if (NDC_code.equals("0045-1525-50")) {
					drug_concept_id = 19082484;
				} else if (NDC_code.equals("0409-1261-30")) {
					drug_concept_id = 19083244;
				} else if (NDC_code.equals("49884-795-01")) {
					drug_concept_id = 1340160;
				} else if (NDC_code.equals("68115-746-10")) {
					drug_concept_id = 19016116;
				} else if (NDC_code.equals("54569-5435-0")) {
					drug_concept_id = 1308306;
				} else if (NDC_code.equals("0703-9416-01")) {
					drug_concept_id = 902769;
				} else if (NDC_code.equals("0088-2220-52")) {
					drug_concept_id = 19099465;
				} else if (NDC_code.equals("0026-0603-20")) {
					drug_concept_id = 19049248;
				} else if (NDC_code.equals("0013-1056-94")) { 
					drug_concept_id = 955641;
				} else if (NDC_code.equals("0074-1949-14")) {
					drug_concept_id = 40162502;
				} else if (NDC_code.equals("55390-053-01")) { 
					drug_concept_id = 1388796;
				} else if (NDC_code.equals("61703-359-91")) {
					drug_concept_id = 1344354;
				} else if (NDC_code.equals("0024-0592-40")) {
					drug_concept_id = 1318011;
				} else if (NDC_code.equals("0781-3282-75")) {
					drug_concept_id = 40059622;
				} else if (NDC_code.equals("0172-3753-96")) {
					drug_concept_id = 19005776;
				} else {
					stmtRxNORM = connRxNORM.createStatement();
					rsRxNORM = stmtRxNORM
							.executeQuery("SELECT RXCUI FROM RXNSAT WHERE ATV='"
									+ NDC_code + "'");
					String RxNORM_code = "";
					if (rsRxNORM.next()) {
						RxNORM_code = rsRxNORM.getString("RXCUI");
					} else {
						System.out.println("FAILED TO MAP NDC to RxNORM for "
								+ NDC_code);
						System.exit(0);
					}

					rsOMOP = stmtOMOP
							.executeQuery("SELECT CONCEPT_ID FROM CONCEPT WHERE CONCEPT_CODE='"
									+ RxNORM_code + "' AND VOCABULARY_ID=8");
					if (rsOMOP.next()) {
						drug_concept_id = rsOMOP.getLong("CONCEPT_ID");
					} else {
						System.out
								.println("FAILED TO GET CONCEPT_ID from CONCEPT Table for RxNORM="
										+ RxNORM_code + " NDC= " + NDC_code);
						System.exit(0);
					}
				}
				rsOMOP = stmtOMOP
						.executeQuery("SELECT VISIT_OCCURRENCE_ID FROM VISIT_OCCURRENCE WHERE PLACE_OF_SERVICE_SOURCE_VALUE='"
								+ rsExact.getString("ENCOUNTER_ID") + "'");
				long visit_occurrence_id = 0;
				if (rsOMOP.next()) {
					visit_occurrence_id = rsOMOP.getLong("VISIT_OCCURRENCE_ID");
				}
				rsOMOP = stmtOMOP
						.executeQuery("SELECT PROVIDER_ID FROM PROVIDER WHERE PROVIDER_SOURCE_VALUE='"
								+ rsExact.getString("ORDER_PROVIDER_ID") + "'");
				long provider_id = 0;
				if (rsOMOP.next()) {
					provider_id = rsOMOP.getLong("PROVIDER_ID");
				}
				int refills = rsExact.getInt("REFILLS");
				int Qty = rsExact.getInt("QTY_ORDERED");
				String sig = rsExact.getString("SIG");

				// Now add MedicationPrescription
//				pstmtOMOP.setLong(1, ++drug_exposure_id);
				pstmtOMOP.setLong(1, person_id);
				pstmtOMOP.setLong(2, drug_concept_id);
				pstmtOMOP.setTimestamp(3, drug_order_ts);
				pstmtOMOP.setNull(4, Types.NULL);
				pstmtOMOP.setLong(5, prescription_written_code);
				pstmtOMOP.setNull(6, Types.NULL);
				pstmtOMOP.setInt(7, refills);
				pstmtOMOP.setInt(8, Qty);
				pstmtOMOP.setNull(9, Types.NULL);
				pstmtOMOP.setString(10, sig);
				if (provider_id == 0) {
					pstmtOMOP.setNull(11, Types.NULL);
				} else {
					pstmtOMOP.setLong(11, provider_id);
				}
				pstmtOMOP.setLong(12, visit_occurrence_id);
				pstmtOMOP.setNull(13, Types.NULL);
				pstmtOMOP.setString(14, NDC_code);

				pstmtOMOP.executeUpdate();

				ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
				tableKeys.next();
				drug_exposure_id = tableKeys.getInt(1);
				
				System.out.println("Adding MedicationPrescription ["
						+ drug_exposure_id + "]");

				pstmtOMOPF.setLong(1, drug_exposure_id);
				pstmtOMOPF.setString(2, rsExact.getString("DOSE"));
				pstmtOMOPF.setString(3, rsExact.getString("UNITS"));
				pstmtOMOPF.executeUpdate();

			}

			stmtExact.close();
			stmtOMOP.close();
			if (stmtRxNORM != null)
				stmtRxNORM.close();
			pstmtOMOP.close();
			pstmtOMOPF.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		} finally {
			try {
				connExact.close();
				connOMOP.close();
				connRxNORM.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void medication_fulfillment_map() {
		Statement stmtExact = null;
		Statement stmtOMOP = null;
		Statement stmtRxNORM = null;
		ResultSet rsExact = null;
		ResultSet rsOMOP = null;
		ResultSet rsRxNORM = null;

		PreparedStatement pstmtOMOP = null;
		PreparedStatement pstmtOMOPF = null;

		setup_database();

		long drug_exposure_id = 0;
		try {
			stmtExact = connExact.createStatement();
			stmtOMOP = connOMOP.createStatement();

			// Find next drug_exposure_id.
//			rsOMOP = stmtOMOP
//					.executeQuery("SELECT DRUG_EXPOSURE_ID FROM DRUG_EXPOSURE ORDER BY DRUG_EXPOSURE_ID DESC");
//			if (rsOMOP.next()) {
//				drug_exposure_id = rsOMOP.getLong("DRUG_EXPOSURE_ID");
//			}

			pstmtOMOP = connOMOP
					.prepareStatement("INSERT INTO DRUG_EXPOSURE (PERSON_ID, "
							+ "DRUG_CONCEPT_ID, DRUG_EXPOSURE_START_DATE, DRUG_EXPOSURE_END_DATE, DRUG_TYPE_CONCEPT_ID, "
							+ "STOP_REASON, REFILLS, QUANTITY, DAYS_SUPPLY, SIG, PRESCRIBING_PROVIDER_ID, VISIT_OCCURRENCE_ID, "
							+ "RELEVANT_CONDITION_CONCEPT_ID, DRUG_SOURCE_VALUE) VALUES "
							+ "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);

			pstmtOMOPF = connOMOP
					.prepareStatement("INSERT INTO F_DRUG_EXPOSURE (DRUG_EXPOSURE_ID, DOSE, UNIT) VALUES "
							+ "(?,?,?)");

			rsExact = stmtExact
					.executeQuery("SELECT * FROM MEDICATION_FULFILLMENT"); // MedicationDispensed
			while (rsExact.next()) {
				String member_id = rsExact.getString("MEMBER_ID");
				rsOMOP = stmtOMOP
						.executeQuery("SELECT PERSON_ID FROM PERSON WHERE PERSON_SOURCE_VALUE='"
								+ member_id + "'");
				long person_id = 0;
				if (rsOMOP.next()) {
					person_id = rsOMOP.getLong("PERSON_ID");
				} else {
					System.out
							.println("Adding MedicationDispense failed - couldn't find person_id");
					continue;
				}

				Timestamp drug_dispense_ts = rsExact
						.getTimestamp("DISPENSE_DATE");
				String NDC_code = rsExact.getString("DRUG_NDC");
				if (NDC_code.equals("67544-134-30")) {
					NDC_code = "0143-1268-01";
				}

				// Check if we have this entry already.
				rsOMOP = stmtOMOP
						.executeQuery("SELECT DRUG_EXPOSURE_ID FROM DRUG_EXPOSURE WHERE"
								+ " PERSON_ID="
								+ person_id
								+ " AND TIMESTAMP(DRUG_EXPOSURE_START_DATE)='"
								+ new Timestamp(drug_dispense_ts.getTime())
								+ "'"
								+ " AND DRUG_SOURCE_VALUE='"
								+ NDC_code
								+ "'"
								+ " AND DRUG_TYPE_CONCEPT_ID="
								+ prescription_dispensed_code); // 38000175 is
																// prescription
																// dispensed.
				if (rsOMOP.next()) {
					System.out.println("MedicationDispense exists ["
							+ rsOMOP.getLong("DRUG_EXPOSURE_ID") + "]");
					continue;
				}

				long drug_concept_id = 0;
				// Get NDC code mapped to RxNORM.
				if (NDC_code.equals("54569-5133-0")) {
					drug_concept_id = 1341927;
				} else if (NDC_code.equals("54868-6054-0")) {// insulin aspart
					drug_concept_id = 1567198;
				} else if (NDC_code.equals("0083-4000-41")) {// valsartan
					drug_concept_id = 1308848;
				} else if (NDC_code.equals("0005-3122-23")) {
					drug_concept_id = 19081499;
				} else if (NDC_code.equals("50580-112-21")) {
					drug_concept_id = 1125393;
				} else if (NDC_code.equals("0045-1525-50")) {
					drug_concept_id = 19082484;
				} else if (NDC_code.equals("0409-1261-30")) {
					drug_concept_id = 19083244;
				} else if (NDC_code.equals("49884-795-01")) {
					drug_concept_id = 1340160;
				} else if (NDC_code.equals("68115-746-10")) {
					drug_concept_id = 19016116;
				} else if (NDC_code.equals("54569-5435-0")) {
					drug_concept_id = 1308306;
				} else if (NDC_code.equals("0703-9416-01")) {
					drug_concept_id = 902769;
				} else if (NDC_code.equals("0088-2220-52")) {
					drug_concept_id = 19099465;
				} else if (NDC_code.equals("0026-0603-20")) {
					drug_concept_id = 19049248;
				} else if (NDC_code.equals("0007-3130-16")) {
					drug_concept_id = 19046610;
				} else if (NDC_code.equals("55045-1642-1")) {
					drug_concept_id = 1362276;
				} else if (NDC_code.equals("0013-1056-94")) { 
					drug_concept_id = 955641;
				} else if (NDC_code.equals("0074-1949-14")) {
					drug_concept_id = 40162502;
				} else if (NDC_code.equals("55390-053-01")) { 
					drug_concept_id = 1388796;
				} else if (NDC_code.equals("61703-359-91")) {
					drug_concept_id = 1344354;
				} else if (NDC_code.equals("0024-0592-40")) {
					drug_concept_id = 1318011;
				} else if (NDC_code.equals("0781-3282-75")) {
					drug_concept_id = 40059622;
				} else if (NDC_code.equals("0172-3753-96")) {
					drug_concept_id = 19005776;
				} else if (NDC_code.equals("50242-056-56")) {
					drug_concept_id = 40099452;
				} else if (NDC_code.equals("54569-5717-0")) {
					drug_concept_id = 40101995;
				} else if (NDC_code.equals("55390-304-05")) {
					drug_concept_id = 40068931;
				} else {
					stmtRxNORM = connRxNORM.createStatement();
					rsRxNORM = stmtRxNORM
							.executeQuery("SELECT RXCUI FROM RXNSAT WHERE ATV='"
									+ NDC_code + "'");
					String RxNORM_code = "";
					if (rsRxNORM.next()) {
						RxNORM_code = rsRxNORM.getString("RXCUI");
					} else {
						System.out.println("FAILED TO MAP NDC to RxNORM for "
								+ NDC_code);
						System.exit(0);
					}

					rsOMOP = stmtOMOP
							.executeQuery("SELECT CONCEPT_ID FROM CONCEPT WHERE CONCEPT_CODE='"
									+ RxNORM_code + "' AND VOCABULARY_ID=8");
					if (rsOMOP.next()) {
						drug_concept_id = rsOMOP.getLong("CONCEPT_ID");
					} else {
						System.out
								.println("FAILED TO GET CONCEPT_ID from CONCEPT Table for RxNORM="
										+ RxNORM_code + " NDC= " + NDC_code);
						System.exit(0);
					}
				}
				rsOMOP = stmtOMOP
						.executeQuery("SELECT VISIT_OCCURRENCE_ID FROM VISIT_OCCURRENCE WHERE PLACE_OF_SERVICE_SOURCE_VALUE='"
								+ rsExact.getString("ENCOUNTER_ID") + "'");
				long visit_occurrence_id = 0;
				if (rsOMOP.next()) {
					visit_occurrence_id = rsOMOP.getLong("VISIT_OCCURRENCE_ID");
				}
				rsOMOP = stmtOMOP
						.executeQuery("SELECT PROVIDER_ID FROM PROVIDER WHERE PROVIDER_SOURCE_VALUE='"
								+ rsExact.getString("PHARMACIST_ID") + "'");
				long provider_id = 0;
				if (rsOMOP.next()) {
					provider_id = rsOMOP.getLong("PROVIDER_ID");
				}
				// int refills = rsExact.getInt("REFILLS");
				int Qty = rsExact.getInt("DISPENSE_QTY");
				String sig = rsExact.getString("SIG");
				int DaysSupply = rsExact.getInt("DAYS_OF_SUPPLY");

				// Now add MedicationDispense
//				pstmtOMOP.setLong(1, ++drug_exposure_id);
				pstmtOMOP.setLong(1, person_id);
				pstmtOMOP.setLong(2, drug_concept_id);
				pstmtOMOP.setTimestamp(3, drug_dispense_ts);
				pstmtOMOP.setNull(4, Types.NULL);
				pstmtOMOP.setLong(5, prescription_dispensed_code);
				pstmtOMOP.setNull(6, Types.NULL);
				pstmtOMOP.setNull(7, Types.NULL); // Refills
				pstmtOMOP.setInt(8, Qty);
				pstmtOMOP.setInt(9, DaysSupply);
				pstmtOMOP.setString(10, sig);
				if (provider_id == 0) {
					pstmtOMOP.setNull(11, Types.NULL);
				} else {
					pstmtOMOP.setLong(11, provider_id);
				}
				pstmtOMOP.setLong(12, visit_occurrence_id);
				pstmtOMOP.setNull(13, Types.NULL);
				pstmtOMOP.setString(14, NDC_code);

				pstmtOMOP.executeUpdate();
				ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
				tableKeys.next();
				drug_exposure_id = tableKeys.getInt(1);
				
				System.out.println("Adding MedicationDispense ["
						+ drug_exposure_id + "]");

				pstmtOMOPF.setLong(1, drug_exposure_id);
				pstmtOMOPF.setString(2, rsExact.getString("DOSE"));
				pstmtOMOPF.setString(3, rsExact.getString("UNITS"));
				pstmtOMOPF.executeUpdate();

			}

			stmtExact.close();
			stmtOMOP.close();
			if (stmtRxNORM != null)
				stmtRxNORM.close();
			pstmtOMOP.close();
			pstmtOMOPF.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				connExact.close();
				connOMOP.close();
				connRxNORM.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void allergy_add_reaction() {
		// map reaction_source_value to SNOMED CT
		Statement stmtOMOP = null;
		Statement stmtOMOP1 = null;
		ResultSet rsOMOP = null;
		
		setup_database();
		try {
			stmtOMOP = connOMOP.createStatement();
			stmtOMOP1 = connOMOP.createStatement();
			
			long allergy_intolerance_reactions_id=0;
//			rsOMOP = stmtOMOP.executeQuery("SELECT ALLERGY_INTOLERANCE_REACTIONS_ID FROM ALLERGY_INTOLERANCE_REACTIONS ORDER BY ALLERGY_INTOLERANCE_REACTIONS_ID DESC");
//			if (rsOMOP.next()) {
//				allergy_intolerance_reactions_id = rsOMOP.getLong("ALLERGY_INTOLERANCE_REACTIONS_ID");
//			}
			rsOMOP = stmtOMOP.executeQuery("SELECT ALLERGY_INTOLERANCE_ID, REACTION_DESCRIPTION FROM ALLERGY_INTOLERANCE");
			PreparedStatement pstatementOMOP = connOMOP.prepareStatement("INSERT INTO ALLERGY_INTOLERANCE_REACTIONS (ALLERGY_INTOLERANCE_ID, REACTION_CONCEPT_ID) VALUES "
					+ "(?,?)", Statement.RETURN_GENERATED_KEYS);

			List<Long> reaction_concept_ids = new ArrayList<Long>();
			while (rsOMOP.next()) {
				reaction_concept_ids.clear();
				long allergy_intolerance_id = rsOMOP.getLong("ALLERGY_INTOLERANCE_ID");
				String reaction_str = rsOMOP.getString("REACTION_DESCRIPTION").toLowerCase();
				if ((Pattern.compile(".*\\brash\\b.*").matcher(reaction_str).matches() 
						&& Pattern.compile(".*\\bskin\\b.*").matcher(reaction_str).matches())
						|| Pattern.compile(".*\\beruption\\b.*").matcher(reaction_str).matches()) {
					// Skin Rash or eruption
					reaction_concept_ids.add(new Long(140214));
				}	
				if (Pattern.compile(".*\\bdiarrhea\\b.*").matcher(reaction_str).matches()) {
					reaction_concept_ids.add(new Long(40304514));
				}
				if (Pattern.compile(".*\\bdizziness\\b.*").matcher(reaction_str).matches()) {
					reaction_concept_ids.add(new Long(40617112));
				} else if (Pattern.compile(".*\\blightheadedness\\b.*").matcher(reaction_str).matches()) {
					reaction_concept_ids.add(new Long(4297376));
				}
				if (Pattern.compile(".*\\bitching\\b.*").matcher(reaction_str).matches()
						&& (Pattern.compile(".*\\beye\\b.*").matcher(reaction_str).matches()
								|| Pattern.compile(".*\\beyes\\b.*").matcher(reaction_str).matches())) {
					reaction_concept_ids.add(new Long(4254270));
				}
				if (Pattern.compile(".*\\bchills\\b.*").matcher(reaction_str).matches()) {
					reaction_concept_ids.add(new Long(434490));
				}
				if (Pattern.compile(".*\\bfever\\b.*").matcher(reaction_str).matches()) {
					reaction_concept_ids.add(new Long(437663));
				}
				if (Pattern.compile(".*\\bhives\\b.*").matcher(reaction_str).matches()
						|| Pattern.compile(".*\\bweal\\b.*").matcher(reaction_str).matches()) {
					reaction_concept_ids.add(new Long(4082588));
				}
				if ((Pattern.compile(".*\\bdiscomfort\\b.*").matcher(reaction_str).matches() 
						|| Pattern.compile(".*\\btightness\\b.*").matcher(reaction_str).matches())
						&& Pattern.compile(".*\\bchest\\b.*").matcher(reaction_str).matches()) {
					// Skin Rash or eruption
					reaction_concept_ids.add(new Long(4133044));
				}
				if (Pattern.compile(".*\\bwheezing\\b.*").matcher(reaction_str).matches()) {
					reaction_concept_ids.add(new Long(314754));
				}
				if (Pattern.compile(".*\\btongue\\b.*").matcher(reaction_str).matches()
						&& Pattern.compile(".*\\bswelling\\b.*").matcher(reaction_str).matches()) {
					reaction_concept_ids.add(new Long(4222234));
				}
				if (Pattern.compile(".*\\bflushing\\b.*").matcher(reaction_str).matches()) {
					reaction_concept_ids.add(new Long(318566));
				}
				if ((Pattern.compile(".*\\bface\\b.*").matcher(reaction_str).matches() 
						|| Pattern.compile(".*\\bfacial\\b.*").matcher(reaction_str).matches())
						&& Pattern.compile(".*\\bswelling\\b.*").matcher(reaction_str).matches()) {
					// Skin Rash or eruption
					reaction_concept_ids.add(new Long(40546664));
				}
				if ((Pattern.compile(".*\\bnose\\b.*").matcher(reaction_str).matches() 
						|| Pattern.compile(".*\\bnosal\\b.*").matcher(reaction_str).matches())
						&& Pattern.compile(".*\\bcongestion\\b.*").matcher(reaction_str).matches()) {
					// Skin Rash or eruption
					reaction_concept_ids.add(new Long(4195085));
				}

				if (reaction_concept_ids.size() > 0) {
					for (long reaction_concept_id : reaction_concept_ids) {
						ResultSet rsOMOP1 = stmtOMOP1.executeQuery("SELECT * FROM ALLERGY_INTOLERANCE_REACTIONS WHERE ALLERGY_INTOLERANCE_ID="+allergy_intolerance_id+" AND REACTION_CONCEPT_ID="+reaction_concept_id);
						if (rsOMOP1.next()) {
							System.out.println("Allergy reaction entry ["+allergy_intolerance_reactions_id+"] exists");
							continue;
						} else {
//							allergy_intolerance_reactions_id++;
//							pstatementOMOP.setLong(1, allergy_intolerance_reactions_id);
							pstatementOMOP.setLong(1, allergy_intolerance_id);
							pstatementOMOP.setLong(2, reaction_concept_id);
							pstatementOMOP.executeUpdate();
							
							ResultSet tableKeys = pstatementOMOP.getGeneratedKeys();
							tableKeys.next();
							allergy_intolerance_id = tableKeys.getInt(1);
							System.out.println("New Reaction["+allergy_intolerance_id+"] is added to AllergyIntolerance["+allergy_intolerance_id+"]");
						}
					}
				} else {
					System.out.println("Couldn't map the allergy reactions, "+reaction_str);					
				}
			}
			stmtOMOP.close();
			stmtOMOP1.close();
			pstatementOMOP.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				connOMOP.close();
				connExact.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	
	private void allergy_map() {
		Statement stmtExact = null;
		Statement stmtOMOP = null;
		Statement stmtRxNORM = null;
		ResultSet rsExact = null;
		ResultSet rsOMOP = null;
		ResultSet rsRxNORM = null;

		PreparedStatement pstmtOMOP = null;

		setup_database();

		try {
			stmtExact = connExact.createStatement();
			stmtOMOP = connOMOP.createStatement();

			long allergy_intolerance_id = 0;
//			rsOMOP = stmtOMOP
//					.executeQuery("SELECT ALLERGY_INTOLERANCE_ID FROM ALLERGY_INTOLERANCE ORDER BY ALLERGY_INTOLERANCE_ID DESC");
//			if (rsOMOP.next()) {
//				allergy_intolerance_id = rsOMOP
//						.getLong("ALLERGY_INTOLERANCE_ID");
//			}

			pstmtOMOP = connOMOP
					.prepareStatement("INSERT INTO ALLERGY_INTOLERANCE (PERSON_ID, REPORTER_PERSON_ID, RECORDER_PERSON_ID, SUBSTANCE_CONCEPT_ID, STATUS, CRITICALITY, CATEGORY, REACTION_DESCRIPTION, LASTOCCURRENCE, ALLERGY_INTOLERANCE_SOURCE_VALUE, COMMENT) VALUES "
							+ "(?,?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
			rsExact = stmtExact.executeQuery("SELECT * FROM ALLERGY");
			while (rsExact.next()) {
				// Get person_id.
				long person_id = 0;
				String member_id = rsExact.getString("MEMBER_ID");
				rsOMOP = stmtOMOP
						.executeQuery("SELECT PERSON_ID FROM PERSON WHERE PERSON_SOURCE_VALUE='"
								+ member_id + "'");
				if (rsOMOP.next()) {
					person_id = rsOMOP.getLong("PERSON_ID");
				} else {
					System.out
							.println("Allergy has no matching person in OMOP database. Member_ID="
									+ member_id);
					continue;
				}

				Date onset_date = rsExact.getDate("ONSET_DATE");

				// Find the RxNORM if it's not RxNORM.
				String concept_vocab_type = rsExact.getString("DRUG_VOCAB");
				String drug_code = rsExact.getString("DRUG_CODE");
				long substance_concept_id = 0;
				if (concept_vocab_type.equalsIgnoreCase("CVX")) {
					// Map CVX to RxNORM.
					rsOMOP = stmtOMOP
							.executeQuery("SELECT SALE_NDC11 FROM NDC2CVX WHERE CVX_CODE='"
									+ drug_code + "'");
					if (rsOMOP.next()) {
						String ndc_code = rsOMOP.getString("SALE_NDC11");

						// // This database has extra 0s
						// ndc_code = ndc_code.replace("00", "0");
						// // remove leading 0
						// ndc_code = ndc_code.replace("-0", "-");

						// Now search this in RxNORM database.
						stmtRxNORM = connRxNORM.createStatement();
						rsRxNORM = stmtRxNORM
								.executeQuery("SELECT RXCUI FROM RXNSAT WHERE ATV='"
										+ ndc_code + "'");
						String RxNORM_code = "";
						if (rsRxNORM.next()) {
							RxNORM_code = rsRxNORM.getString("RXCUI");
						} else {
							System.out
									.println("FAILED TO MAP NDC to RxNORM for NDC="
											+ ndc_code
											+ " and CVX="
											+ drug_code);
							System.exit(0);
						}
						rsOMOP = stmtOMOP
								.executeQuery("SELECT CONCEPT_ID FROM CONCEPT WHERE CONCEPT_CODE='"
										+ RxNORM_code + "' AND VOCABULARY_ID=8");
						if (rsOMOP.next()) {
							substance_concept_id = rsOMOP.getLong("CONCEPT_ID");
						} else {
							System.out
									.println("FAILED TO GET CONCEPT_ID from CONCEPT Table for RxNORM="
											+ RxNORM_code
											+ " NDC= "
											+ ndc_code
											+ " and CVX= " + drug_code);
							System.exit(0);
						}
					}
				}

				if (concept_vocab_type.equalsIgnoreCase("RXNORM")) {
					rsOMOP = stmtOMOP
							.executeQuery("SELECT CONCEPT_ID FROM CONCEPT WHERE CONCEPT_CODE='"
									+ drug_code + "' AND VOCABULARY_ID=8");
					if (rsOMOP.next()) {
						substance_concept_id = rsOMOP.getLong("CONCEPT_ID");
					} else {
						System.out
								.println("FAILED TO GET CONCEPT_ID from CONCEPT Table for RxNORM="
										+ drug_code);
						System.exit(0);
					}

				}

				if (substance_concept_id == 0) {
					System.out
							.println("Allergy concept code is not parsable... 0 will be used");
				}

				// First check if this exists.
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				String dfs = df.format(onset_date.getTime());

				rsOMOP = stmtOMOP
						.executeQuery("SELECT * FROM ALLERGY_INTOLERANCE WHERE PERSON_ID="
								+ person_id
								+ " "
								+ "AND SUBSTANCE_CONCEPT_ID="
								+ substance_concept_id
								+ " "
								+ "AND DATE(LASTOCCURRENCE)='" + dfs + "'");
				if (rsOMOP.next()) {
					System.out.println("Allergy [" + allergy_intolerance_id
							+ "] exists");
					continue;
				}

				String category = "";
				if (rsExact.getString("ALLERGY_TYPE").equalsIgnoreCase("DRUG")) {
					category = "medication";
				} else if (rsExact.getString("ALLERGY_TYPE").equalsIgnoreCase(
						"FOOD")) {
					category = "food";
				} else {
					category = "environment";
				}

				String criticality = "unassessible";
				String severity = rsExact.getString("SEVERITY_DESCRIPTION");
				if (severity.contains("critical")) {
					criticality = "high";
				} else if (severity.contains("moderate")) {
					criticality = "low";
				}
				String reaction_desc = rsExact.getString("REACTION");

//				pstmtOMOP.setLong(1, ++allergy_intolerance_id);
				pstmtOMOP.setLong(1, person_id);
				if (rsExact.getString("INFORMATION_SOURCE").equalsIgnoreCase(
						"patient")) {
					pstmtOMOP.setLong(2, person_id);
				} else {
					pstmtOMOP.setNull(2, Types.NULL);
				}
				pstmtOMOP.setNull(3, Types.NULL);
				pstmtOMOP.setLong(4, substance_concept_id);
				pstmtOMOP.setString(5, "confirmed");
				pstmtOMOP.setString(6, criticality);
				pstmtOMOP.setString(7, category);
				if (reaction_desc != null && !reaction_desc.isEmpty())
					pstmtOMOP.setString(8, reaction_desc);
				else 
					pstmtOMOP.setNull(8, Types.NULL);
				pstmtOMOP.setDate(9, onset_date);
				pstmtOMOP.setString(10, drug_code);
				pstmtOMOP.setString(11, rsExact.getString("ALLERGEN"));

				pstmtOMOP.executeUpdate();
				ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
				tableKeys.next();
				allergy_intolerance_id = tableKeys.getInt(1);
				System.out.println("Adding Allergy [" + allergy_intolerance_id
						+ "]");

			}

			stmtExact.close();
			stmtOMOP.close();
			pstmtOMOP.close();
			if (stmtRxNORM != null)
				stmtRxNORM.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				connExact.close();
				connOMOP.close();
				connRxNORM.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private void immunization_map() {
		Statement stmtExact = null;
		Statement stmtOMOP = null;
		Statement stmtRxNORM = null;
		ResultSet rsExact = null;
		ResultSet rsOMOP = null;
		ResultSet rsRxNORM = null;

		PreparedStatement pstmtOMOP = null;

		setup_database();

		try {
			stmtExact = connExact.createStatement();
			stmtOMOP = connOMOP.createStatement();
			stmtRxNORM = connRxNORM.createStatement();

			long immunization_id = 0;
//			rsOMOP = stmtOMOP
//					.executeQuery("SELECT IMMUNIZATION_ID FROM IMMUNIZATION ORDER BY IMMUNIZATION_ID DESC");
//			if (rsOMOP.next()) {
//				immunization_id = rsOMOP.getLong("IMMUNIZATION_ID");
//			}

			pstmtOMOP = connOMOP
					.prepareStatement("INSERT INTO IMMUNIZATION (PERSON_ID, ADMINISTERED_DATE, "
							+ "VACCINE_CONCEPT_ID, WASNOTGIVEN, REPORTED, PERFORMER_PROVIDER_ID, REQUESTER_PROVIDER_ID, DISPLAY, "
							+ "VISIT_OCCURRENCE_ID, MANUFACTURER_PROVIDER_ID, LOTNUMBER, DOSEQTY, EXPLANATION_REASON_CONCEPT_ID, "
							+ "EXPLANATION_REASONNOTGIVEN_CONCEPT_ID, IMMUNIZATION_SOURCE_VALUE) VALUES "
							+ "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
			rsExact = stmtExact.executeQuery("SELECT * FROM IMMUNIZATION");
			while (rsExact.next()) {
				String member_id = rsExact.getString("MEMBER_ID");
				long person_id = 0;
				rsOMOP = stmtOMOP
						.executeQuery("SELECT PERSON_ID FROM PERSON WHERE PERSON_SOURCE_VALUE='"
								+ member_id + "'");
				if (rsOMOP.next()) {
					person_id = rsOMOP.getLong("PERSON_ID");
				} else {
					System.out
							.println("IMMUNIZATION couldn't find matching person_id for member_id="
									+ member_id);
					continue;
				}

				long visit_occurrence_id = 0;
				rsOMOP = stmtOMOP
						.executeQuery("SELECT VISIT_OCCURRENCE_ID FROM VISIT_OCCURRENCE WHERE PLACE_OF_SERVICE_SOURCE_VALUE='"
								+ rsExact.getString("ENCOUNTER_ID") + "'");
				if (rsOMOP.next()) {
					visit_occurrence_id = rsOMOP.getLong("VISIT_OCCURRENCE_ID");
				}

				Timestamp immunization_ts = rsExact
						.getTimestamp("VACCINATION_DATE");
				long provider_id = 0;
				rsOMOP = stmtOMOP
						.executeQuery("SELECT PROVIDER_ID FROM PROVIDER WHERE PROVIDER_SOURCE_VALUE='"
								+ rsExact.getString("PROVIDER_ID") + "'");
				if (rsOMOP.next()) {
					provider_id = rsOMOP.getLong("PROVIDER_ID");
				}

				String cvx_code = rsExact.getString("VACCINE_CVX");
				// find the RxNORM code for vaccine_concept.
				// Map CVX to RxNORM.
				long vaccine_concept_id = 0;

				rsOMOP = stmtOMOP
						.executeQuery("SELECT SALE_NDC11 FROM NDC2CVX WHERE CVX_CODE='"
								+ cvx_code + "'");
				Statement stmtOMOP1 = connOMOP.createStatement();
				while (rsOMOP.next()) {
					String ndc_code = rsOMOP.getString("SALE_NDC11");

					// // This database has extra 0s
					// ndc_code = ndc_code.replace("00", "0");
					// // remove leading 0
					// ndc_code = ndc_code.replace("-0", "-");
					if (ndc_code.equalsIgnoreCase("49281-0400-05")) {
						ndc_code = "49281-400-05"; // :( darn...
					} 

					// Now search this in RxNORM database.
					stmtRxNORM = connRxNORM.createStatement();
					rsRxNORM = stmtRxNORM
							.executeQuery("SELECT RXCUI FROM RXNSAT WHERE ATV='"
									+ ndc_code + "'");
					String RxNORM_code = "";
					while (rsRxNORM.next()) {
						RxNORM_code = rsRxNORM.getString("RXCUI");
						ResultSet rsOMOP1 = stmtOMOP1
								.executeQuery("SELECT CONCEPT_ID FROM CONCEPT WHERE CONCEPT_CODE='"
										+ RxNORM_code + "' AND VOCABULARY_ID=8");
						if (rsOMOP1.next()) {
							vaccine_concept_id = rsOMOP1.getLong("CONCEPT_ID");
							break;
						} else {
							System.out
									.println("IMMUNIZATION FAILED TO GET CONCEPT_ID from CONCEPT Table for RxNORM="
											+ RxNORM_code
											+ " NDC= "
											+ ndc_code
											+ " and CVX= " + cvx_code);
						}
					}
					if (vaccine_concept_id == 0) {
						System.out
								.println("IMMUNIZATION FAILED TO MAP NDC to RxNORM for NDC="
										+ ndc_code + " and CVX=" + cvx_code);
					} else {
						break;
					}
				}
				stmtOMOP1.close();

				if (vaccine_concept_id == 0) {
					System.exit(0);
				}

				rsOMOP = stmtOMOP
						.executeQuery("SELECT * FROM IMMUNIZATION WHERE PERSON_ID="
								+ person_id
								+ " AND "
								+ "VACCINE_CONCEPT_ID="
								+ vaccine_concept_id
								+ " AND "
								+ "TIMESTAMP(ADMINISTERED_DATE)='"
								+ new Timestamp(immunization_ts.getTime())
								+ "'");
				if (rsOMOP.next()) {
					System.out.println("IMMUNIZATION [" + immunization_id + 1
							+ "] exists");
					continue;
				}

//				String manufact = rsExact.getString("MANUFACTURER");
				String lot_number = rsExact.getString("LOT_NUMBER");

//				pstmtOMOP.setLong(1, ++immunization_id);
				pstmtOMOP.setLong(1, person_id);
				pstmtOMOP.setTimestamp(2, immunization_ts);
				pstmtOMOP.setLong(3, vaccine_concept_id);
				pstmtOMOP.setInt(4, 0);
				pstmtOMOP.setInt(5, 1);
				pstmtOMOP.setLong(6, provider_id);
				pstmtOMOP.setNull(7, Types.NULL);
				pstmtOMOP.setString(8, rsExact.getString("VACCINE_NAME"));
				pstmtOMOP.setLong(9, visit_occurrence_id);
				pstmtOMOP.setNull(10, Types.NULL);
				pstmtOMOP.setString(11, lot_number);
				pstmtOMOP.setString(12, rsExact.getString("DOSE"));
				pstmtOMOP.setNull(13, Types.NULL);
				pstmtOMOP.setNull(14, Types.NULL);
				pstmtOMOP.setString(15, cvx_code);

				pstmtOMOP.executeUpdate();
				ResultSet tableKeys = pstmtOMOP.getGeneratedKeys();
				tableKeys.next();
				immunization_id = tableKeys.getInt(1);
				
				System.out.println("Adding immunization [" + immunization_id
						+ "] added");
			}

			stmtExact.close();
			stmtOMOP.close();
			stmtRxNORM.close();
			pstmtOMOP.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				connExact.close();
				connOMOP.close();
				connRxNORM.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private void death_map() {
		Statement stmtExact = null;
		Statement stmtOMOP = null;
		ResultSet rsExact = null;
		ResultSet rsOMOP = null;
		PreparedStatement pstmtOMOP = null;

		setup_database();
	
		try {
			stmtExact = connExact.createStatement();
			stmtOMOP = connOMOP.createStatement();
			rsExact = stmtExact.executeQuery("SELECT * FROM DEATH");
			pstmtOMOP = connOMOP.prepareStatement("INSERT INTO (PERSON_ID, DEATH_DATE, DEATH_TYPE_CONCEPT_ID) VALUES (?,?,?)");
			while (rsExact.next()) {
				String member_id = rsExact.getString("MEMBER_ID");
				rsOMOP = stmtOMOP.executeQuery("SELECT PERSON_ID FROM PERSON WHERE PERSON_SOURCE_VALUE='"+member_id+"'");
				long person_id = 0;
				if (rsOMOP.next()) {
					person_id = rsOMOP.getLong("PERSON_ID");
				} else {
					System.out.println("DEATH - couldn't match person_id");
					continue;
				}
				
				rsOMOP = stmtOMOP.executeQuery("SELECT * FROM DEATH WHERE PERSON_ID="+person_id);
				if (rsOMOP.next()) {
					System.out.println("DEATH for person_id="+person_id+", member_id="+member_id+" exists");
					continue;
				}
				
				Timestamp death_ts = rsExact.getTimestamp("DOD");
				
				pstmtOMOP.setLong(1, person_id);
				pstmtOMOP.setDate(2, new Date (death_ts.getTime()));
				pstmtOMOP.setLong(3, 38003569);
				
				System.out.println("Adding DEATH for person="+person_id);
				pstmtOMOP.executeUpdate();
			}
			
			stmtExact.close();
			stmtOMOP.close();
			pstmtOMOP.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				connExact.close();
				connOMOP.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	public static void main(String[] args) {
		DBMapMain myClass = new DBMapMain();
		System.out.println("--- Populating Provider/CareSite/Organization");
		myClass.provider_map();

		System.out.println("--- Populating Persons ---");
		myClass.enrollment_map();

		System.out.println("--- Populating Conditions ---");
		myClass.problem_map();

		System.out.println("--- Populating Encounters ---");
		myClass.encounters_map();

		System.out.println("--- Populating Vitals ---");
		myClass.vitals_map();

		System.out.println("--- Populating Lab Data ---");
		myClass.labs_map();

		System.out.println("--- Populating MedicationPrescription ---");
		myClass.medication_orders_map();

		System.out.println("--- Populating MedicationDispense ---");
		myClass.medication_fulfillment_map();

		System.out.println("--- Populating Allergies ---");
		myClass.allergy_map();
		myClass.allergy_add_reaction();
		

		System.out.println("--- Populating Immunizations ---");
		myClass.immunization_map();

		System.out.println("--- Populating Deaths ---");
		myClass.death_map();
	}

}
