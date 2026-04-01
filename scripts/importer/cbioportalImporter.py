def remove_samples(jvm_args, study_ids, sample_ids):
    """
    Remove samples AND also delete patients if they become sample-less
    """
    args = jvm_args.split(' ')
    args.append(REMOVE_SAMPLES_CLASS)
    args.append("--study_ids")
    args.append(study_ids)
    args.append("--sample_ids")
    args.append(sample_ids)
    run_java(*args)



    try:
       
        from .cbioportal_common import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

      
        query = """
            SELECT DISTINCT patient_id
            FROM sample
            WHERE cancer_study_identifier = %s
        """
        cursor.execute(query, (study_ids,))
        all_patients = {row[0] for row in cursor.fetchall()}

      
        patients_to_delete = []

        for patient in all_patients:
            cursor.execute("""
                SELECT COUNT(*) FROM sample
                WHERE cancer_study_identifier = %s AND patient_id = %s
            """, (study_ids, patient))

            count = cursor.fetchone()[0]

            if count == 0:
                patients_to_delete.append(patient)

       
        if patients_to_delete:
            patient_ids_str = ",".join(patients_to_delete)

            args = jvm_args.split(' ')
            args.append(REMOVE_PATIENTS_CLASS)
            args.append("--study_ids")
            args.append(study_ids)
            args.append("--patient_ids")
            args.append(patient_ids_str)

            run_java(*args)

            print(f"Removed sample-less patients: {patient_ids_str}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Warning: Could not clean up sample-less patients: {e}", file=ERROR_FILE)
