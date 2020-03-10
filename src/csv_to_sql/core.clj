(ns csv-to-sql.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc])
  (:import org.apache.commons.io.input.BOMInputStream
           org.apache.commons.io.ByteOrderMark))

(def db-spec {
  :dbtype "mysql"
  :dbname (System/getenv "DB_NAME_RED_PROD_ICL")
  :user (System/getenv "DB_USER_RED_PROD_ICL")
  :host (System/getenv "DB_HOST_RED_PROD_ICL")
  :password (System/getenv "DB_PASSWORD_RED_PROD_ICL")
  })

(def csv-file-to-sql-table {
  "resources/research_themes.csv" "research_themes"
  "resources/research_programmes.csv" "research_programmes"
  "resources/people.csv" "people"
  "resources/projects.csv" "studies"
  "resources/publications.csv" "publications"
  "resources/research_categories.csv" "research_categories"
  "resources/staff_categories.csv" "staff_categories"
  "resources/ukcrc_categories.csv" "ukcrc_categories"
  })

(defn friendly-mysql-name
  [text]
  (str/join "_"
    (str/split (str/lower-case text) #"\s")))

(defn csv-header-to-sql-column
  [header]
  (friendly-mysql-name header))

(defn read-csv
  [file]
  (with-open [reader (-> file
                       io/input-stream
                       BOMInputStream.
                       io/reader)]
  (doall (csv/read-csv reader))))

(defn csv-data->maps [csv-data]
  (map zipmap
       (->> (first csv-data)
            (map csv-header-to-sql-column)
            (map keyword)
            repeat)
            (rest csv-data)))

; FIXME: Ignore duplicates or import only once
(defn import-csv
  [table rows]
  (jdbc/insert-multi! db-spec table rows))

(defn ukcrc-category
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM ukcrc_categories WHERE name = ? LIMIT 1" name])) :id))

(defn role
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM roles WHERE name = ? LIMIT 1" name])) :id))

(defn research-theme
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM research_themes WHERE name = ? LIMIT 1" name])) :id))

; FIXME: research programmes table does not exist on red icl
(defn research-programme
  [name]
  (if (empty? name)
    nil
    (get (first (jdbc/query db-spec ["SELECT id FROM research_programmes WHERE name = ? LIMIT 1" name])) :id)))

(defn research-type
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM research_types WHERE name = ? LIMIT 1" name])) :id))

(defn lead-centre
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM lead_centres WHERE name = ? LIMIT 1" name])) :id))

(defn study-type
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM study_types WHERE name = ? LIMIT 1" name])) :id))

; FIXME: use date directly instead of string date
(defn study-year
  [date]
  (if (empty? date)
    nil
    (.format (java.text.SimpleDateFormat. "yyyy") (.parse
        (java.text.SimpleDateFormat. "yyyy-MM-dd")
        date)))
  )

; FIXME: is this format correct
(defn current-financial-year
  [date]
  (if-not (nil? date)
    (.format (java.text.SimpleDateFormat. "yyyy") date))
  )

(defn parse-orcid
  [orcid]
  (str/join (str/split
              (str/trim orcid) #"-")))

(defn parse-title
  [title]
  (if (= "prof" (str/lower-case (str/trim title)))
    "Professor"
    title))

(defn intervention
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM interventions WHERE name = ? LIMIT 1" name])) :id))

(defn project-impact
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM project_impacts WHERE name = ? LIMIT 1" name])) :id))

(defn project-purpose
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM project_purposes WHERE name = ? LIMIT 1" name])) :id))

(defn yes-no-to-int
  [value]
  (cond
    (= "yes" (str/lower-case (str/trim value))) 1
    (= "no" (str/lower-case (str/trim value))) 0))

(defn study-status
  [value]
  (cond
    (= "In Set-Up" value) "In set up"
    :else (str/trim value)))

(defn on-portfolio
  [value]
  (cond
    (= true (empty? value)) "Unknown"
    :else (str/trim value)))

; FIXME: date may not always be in desired format
(defn String->Date
  [date]
  (if (empty? date)
    nil
    (.format
      (java.text.SimpleDateFormat. "yyyy-MM-dd")
      (.parse (java.text.SimpleDateFormat. "dd/MM/yyyy") date)
      )))

; FIXME: date may not always be in desired format
(defn published-on
  [date]
  (String->Date date))

(defn String->Double [str]
  (if (not-empty str)
    (Double/parseDouble str)))

(defn funding-category-name
  [name]
  (cond
    (= "brc funded" (str/lower-case name)) "BRC Funding"
    (= "research charity" (str/lower-case name)) "Research Charity"
    (= "research council" (str/lower-case name)) "Research Council"
    (= "industry collaborative" (str/lower-case name)) "Industry Collaborative"
    (= "industry contract" (str/lower-case name)) "Industry Contract"
    (= "dhsc/nihr" (str/lower-case name)) "DH/NIHR"
    (= "other non-commercial" (str/lower-case name)) "Other Non-Commericial"
    ))

(defn funding-industry-sector
  [name]
  ; (println (str "Funding industry sector = " name))
  (cond
    (= "industry sector" (str/lower-case name)) "Industry Sector"
    (= "pharma" (str/lower-case name)) "Pharma"
    (= "biotech" (str/lower-case name)) "Biotech"
    (= "medtech/device" (str/lower-case name)) "Medtech/Device"
    (= "in vitro diagnostic" (str/lower-case name)) "In vitro diagnostic"
    (= "contract research organisation" (str/lower-case name)) "Contract Research Organisation"
    (= "non-life sciences company" (str/lower-case name)) "Non-life sciences company"
    ))

(defn nihr-research-programme
  [name]
  (cond
    (= "eme" (str/lower-case name)) "Efficacy and Mechanism Evaluation (EME)"
    (= "hs&dr" (str/lower-case name)) "Health Service & Delivery Research Programme (HS&DR)"
    (= "hta" (str/lower-case name)) "Health Technology Assessment (HTA)"
    (= "i4i" (str/lower-case name)) "Invention for Innovation (i4i)"
    (= "pgfar" (str/lower-case name)) "Programme Grants for Applied Research (PGfAR)"
    (= "other infrastructure" (str/lower-case name)) "Other infrastructure"
    (= "other nihr funding" (str/lower-case name)) "Other NIHR funding"
    ; (= "Public Health Research (PHR)" (str/lower-case name)) "Public Health Research (PHR)"
    ; (= "Research for Patient Benefit (RfPB)" (str/lower-case name)) "Research for Patient Benefit (RfPB)"
    ; (= "NIHR Research Schools" (str/lower-case name)) "NIHR Research Schools"
    ))

; FIXME: verify the year of a stutus is current financial year
; FIXME: verify what to do about missing statuses
(defn create-study-status
  [study row]
  (def duplicate (first (jdbc/query db-spec ["SELECT * FROM study_statuses WHERE study_id = ? LIMIT 1" (get study :id)])))
  (if (nil? duplicate)
    (if-not (empty? (get row :status))
      (if-not (= "Closed" (study-status (get row :status)))
        (do (println "No Study Status duplicate")
            (jdbc/execute! db-spec ["INSERT INTO study_statuses SET
                                                study_id = ?,
                                                year = ?,
                                                status = ?"
                                                (get study :id),
                                                (current-financial-year (java.util.Date.)),
                                                (study-status (get row :status))
                                                ])
                                                )
        (println "Skipped:: Status is Closed"))
      (println "Skipping import: Study has no status"))
    (println "Duplicate Study Status found"))
  )

; if duplicate is not present
; then create lead centre and return id
; else return duplicate id
(defn create-lead-centre
  [name]
  (def duplicate (first (jdbc/query db-spec ["SELECT id FROM lead_centres WHERE name = ? LIMIT 1" name])))

  (if (nil? duplicate)
    (do
      (println "No sponsor duplicate")
      (println (str "Inserting lead center - " name))
      (jdbc/execute! db-spec ["INSERT INTO lead_centres SET
                                        name = ?",
                                        name]))
    (do
      (println "Duplicate sponsor found")
      (:id duplicate)
      ))
  )

(defn create-organisation-involvement
  [study row]
  (def lead-centre-id (lead-centre (get row :lead_centre_id)))
  (def sponsor-id (create-lead-centre (get row :sponsor)))
  (def link-to-nihr-translational-research-collaboration (yes-no-to-int (get row :link_to_nihr_translational_research_collaboration)))
  (def link-to-crf-cross-cutting-theme (yes-no-to-int (get row :project_link_to_crf_cct)))
  (def link-to-crf-explanation (get row :project_link_to_crf_cct_detail))
  (def duplicate (first (jdbc/query db-spec ["SELECT study_id FROM organisation_involvement WHERE study_id = ? LIMIT 1" (get study :id)])))

  (if (nil? duplicate)
    (do
      (println "No Organisation Involvement duplicate")
      (jdbc/execute! db-spec ["INSERT INTO organisation_involvement SET
                                        study_id = ?,
                                        lead_centre_id = ?,
                                        sponsor_id = ?,
                                        link_to_nihr_translational_research_collaboration = ?,
                                        link_to_crf_cross_cutting_theme = ?,
                                        link_to_crf_explanation = ?",
                                        (get study :id),
                                        lead-centre-id,
                                        sponsor-id,
                                        link-to-nihr-translational-research-collaboration,
                                        link-to-crf-cross-cutting-theme,
                                        link-to-crf-explanation
                                        ]))
    (println "Duplicate Organisation Involvement found"))
  )

(defn create-study-assignment
  [study row]
  (def title (parse-title (get row :project_lead_title)))
  (def first-name (get row :firstname))
  (def surname (get row :surname))
  (def person (first (jdbc/query db-spec ["SELECT * FROM people WHERE title = ? AND first_name = ? AND surname = ? LIMIT 1" title first-name surname])))
  (def acting-capacity (first (jdbc/query db-spec ["SELECT id FROM acting_capacities WHERE name = ? LIMIT 1" "Principal Investigator"])))
  (def duplicate (first (jdbc/query db-spec ["SELECT study_id FROM study_assignments WHERE study_id = ? AND acting_capacity_id = ? AND person_id = ? LIMIT 1" (:id study) (:id acting-capacity) (:id person)])))

  (if (nil? duplicate)
    (if-not (nil? person)
      (if-not (empty? (:start_date row))
        (do
          (println "No duplicate Study Assignment")
          (jdbc/execute! db-spec ["INSERT INTO study_assignments SET
                                            study_id = ?,
                                            person_id = ?,
                                            acting_capacity_id = ?,
                                            started_on = ?",
                                            (:id study),
                                            (:id person),
                                            (:id acting-capacity)
                                            (String->Date (:start_date row))
                                            ]))
        (println "Skipping Study Assignment: PI started on (study start date) is not present"))
      (println "Skipping Study Assignment: PI is not present"))
    (println "Duplicate Study Assignment found"))
  )

(defn create-recruitment
  [study row]
  (def duplicate (first (jdbc/query db-spec ["SELECT study_id FROM recruitment WHERE study_id = ? AND year = ? LIMIT 1" (:id study) "2019"])))

  (if (nil? duplicate)
    (do
      (println "No duplicate Recruitment")
      (jdbc/execute! db-spec ["INSERT INTO recruitment SET
                                        study_id = ?,
                                        year = ?,
                                        total = ?",
                                        (:id study),
                                        "2019",
                                        (String->Double (:participants_recruited_to_centre_fy_18_19 row))
                                        ]))
    (println "Duplicate Recruitment found"))
  )

(defn create-study-funding-default
  [row funding-category study duplicate]
  (println "Creating funding default")
  (if (nil? duplicate)
    (do
      (println "No Study Funding duplicate found")
      (jdbc/execute! db-spec ["INSERT INTO study_funding set
                                main_funding_source = ?,
                                funding_category_id = ?,
                                study_id = ?,
                                total_external_funding_awarded_to_the_brc = ?"
                                (:main_funding_source row)
                                (:id funding-category)
                                (:id study)
                                (String->Double (:total_external_funding_awarded row))
                                ])
      )
    (println "Duplicate Study Funding found")
    )
  )

(defn create-study-funding-brc
  [row funding-category study duplicate]
  (println "Creating funding BRC")
  (if (nil? duplicate)
    (jdbc/execute! db-spec ["INSERT INTO study_funding set
                            main_funding_source = ?,
                            funding_category_id = ?,
                            study_id = ?,
                            brc_funding_amount = ?,
                            total_external_funding_awarded_to_the_brc = ?"
                            (:main_funding_source row)
                            (:id funding-category)
                            (:id study)
                            (String->Double (:main_funding_brc row))
                            (String->Double (:total_external_funding_awarded row))
                            ])
    (println "Duplicate Study Funding found")
    ))

(defn create-study-funding-industry
  [row funding-category study duplicate]
  (println "Creating funding Industry")
  (println (:main_funding_industry_collaborative_or_industry_contract row))
  (if (nil? duplicate)
    (jdbc/execute! db-spec ["INSERT INTO study_funding set
                            main_funding_source = ?,
                            funding_category_id = ?,
                            study_id = ?,
                            industry_sector = ?,
                            total_external_funding_awarded_to_the_brc = ?"
                            (:main_funding_source row)
                            (:id funding-category)
                            (:id study)
                            (funding-industry-sector (:main_funding_industry_collaborative_or_industry_contract row))
                            (String->Double (:total_external_funding_awarded row))
                            ])
    (println "Duplicate Study Funding found")
    )
  )

(defn create-study-funding-dhsc-nihr
  [row funding-category study duplicate]
  (println "Creating funding DI/NIHR")
  (if (nil? duplicate)
    (jdbc/execute! db-spec ["INSERT INTO study_funding set
                            main_funding_source = ?,
                            funding_category_id = ?,
                            study_id = ?,
                            nihr_research_programme = ?,
                            total_external_funding_awarded_to_the_brc = ?"
                            (:main_funding_source row)
                            (:id funding-category)
                            (:id study)
                            (nihr-research-programme (:main_funding_dhsc_or_nihr row))
                            (String->Double (:total_external_funding_awarded row))
                            ])
    (println "Duplicate Study Funding found")
    )
  )

(defn create-study-funding
  [study row]
  (def study-title (get row :title))
  (def study (first (jdbc/query db-spec ["SELECT id FROM studies WHERE title = ? LIMIT 1" study-title])))
  (def funding-category (first (jdbc/query db-spec ["SELECT id, name FROM funding_categories WHERE name = ? LIMIT 1" (funding-category-name (:main_funding_category row))])))
  (def duplicate (first (jdbc/query db-spec ["SELECT * FROM study_funding WHERE study_id = ? LIMIT 1" (get study :id)])))

  (cond
    (= "BRC Funding" (:name funding-category)) (create-study-funding-brc row funding-category study duplicate)
    (= "Industry Collaborative" (:name funding-category)) (create-study-funding-industry row funding-category study duplicate)
    (= "Industry Contract" (:name funding-category)) (create-study-funding-industry row funding-category study duplicate)
    (= "DH/NIHR" (:name funding-category)) (create-study-funding-dhsc-nihr row funding-category study duplicate)
    :else (create-study-funding-default row funding-category study duplicate)
    ))

(defn create-person-roles
  [person role-id]
  (def duplicate (first (jdbc/query db-spec ["SELECT * FROM people_roles WHERE person_id = ? AND role_id = ? LIMIT 1" (get person :id) role-id])))
  (if (nil? duplicate)
    (do
      (println "No duplicate")
      (if-not (nil? role-id)
        (jdbc/execute! db-spec ["insert ignore into red_icl_production.people_roles set person_id = ?, role_id = ?", (get person :id), role-id])
        (println "SKIPPED: missing role id"))
      )
      (println "SKIPPED: duplicate"))
  )

; FIXME: confirm object type format (lower case singular of table name)
(defn create-person-theme-associations
  [person research-theme-id]
  (def duplicate (first (jdbc/query db-spec ["SELECT id FROM theme_associations WHERE object_id = ? AND object_type = ? AND theme_id = ? LIMIT 1" (get person :id), "person", research-theme-id])))
  (if (nil? duplicate)
        (if-not (nil? research-theme-id)
          (do
            (println "No duplicate found")
            (jdbc/execute! db-spec ["INSERT INTO theme_associations SET object_id = ?,
                                                                               object_type = ?,
                                                                               theme_id = ?",
                                                                               (get person :id),
                                                                               "person",
                                                                               research-theme-id
                                                                               ]))
          (println "Skipping import: Person has no research theme"))
        (println "Duplicate found")
        )
  )

(defn pre-process-studies
  [csv-data-maps]
  (->> csv-data-maps
    (map #(dissoc %
      :main_funding_source
      :surname
      :project_link_to_crf_cct_detail
      :main_funding_brc
      :participants_recruited_to_centre_fy_18_19
      :lead_centre_id
      :link_to_nihr_translational_research_collaboration
      :firstname
      :orcid_id
      :status
      :total_external_funding_awarded
      :main_funding_dhsc_or_nihr
      :main_funding_category
      :project_link_to_crf_cct
      :contract_research_organisation
      :project_lead_title
      :main_funding_industry_collaborative_or_industry_contract
      :sponsor
      ))

    (map (fn [csv-record]
           (update csv-record :research_theme_id #(research-theme %))))
    (map (fn [csv-record]
           (update csv-record :iras_id #(String->Double %))))
    (map (fn [csv-record]
           (update csv-record :start_date #(String->Date %))))
    (map (fn [csv-record]
           (update csv-record :end_date #(String->Date %))))
    (map (fn [csv-record]
          (update csv-record :ukcrc_category_id #(ukcrc-category %))))
    (map (fn [csv-record]
          (update csv-record :research_theme_id #(research-theme %))))
    (map (fn [csv-record]
          (update csv-record :research_programme_id #(research-programme %))))
    (map (fn [csv-record]
          (update csv-record :research_type_id #(research-type %))))
    (map (fn [csv-record]
          (update csv-record :randomised #(yes-no-to-int %))))
    (map (fn [csv-record]
          (update csv-record :first_in_man #(yes-no-to-int %))))
    (map (fn [csv-record]
          (update csv-record :rec_approval_required #(yes-no-to-int %))))
    (map (fn [csv-record]
          (update csv-record :study_type_id #(study-type %))))
    (map (fn [csv-record]
          (update csv-record :primary_intervention_id #(intervention %))))
    (map (fn [csv-record]
          (update csv-record :project_impact_id #(project-impact %))))
    (map (fn [csv-record]
          (update csv-record :project_purpose_id #(project-purpose %))))
    (map (fn [csv-record]
          (update csv-record :on_portfolio #(on-portfolio %))))
    ))

(defn pre-process-people
  [csv-data-maps]
  (->> csv-data-maps
    (map #(dissoc %
      :role_id
      :primary_theme
      ))
    (map (fn [csv-record]
           (update csv-record :title #(parse-title %))))
    (map (fn [csv-record]
           (update csv-record :fte #(String->Double %))))
    (map (fn [csv-record]
           (update csv-record :ukcrc_category_id #(ukcrc-category %))))
    (map (fn [csv-record]
           (update csv-record :orcid_id #(parse-orcid %))))
    ))

(defn pre-process-publications
  [csv-data-maps]
  (->> csv-data-maps
    (map (fn [csv-record]
           (update csv-record :research_theme_id #(research-theme %))))
    (map (fn [csv-record]
           (update csv-record :published_on #(published-on %))))
    (map (fn [csv-record]
           (update csv-record :nihr_acknowledgement #(yes-no-to-int %))))
    (map (fn [csv-record]
           (update csv-record :open_access #(yes-no-to-int %))))
    ))

(defn post-process-studies
  [raw-csv-data]
  (doseq [row raw-csv-data]
    (def study-title (get row :title))
    (def study (first (jdbc/query db-spec ["SELECT id FROM studies WHERE title = ? LIMIT 1" study-title])))
    (println (str "Study ID: " (:id study)))
    (create-study-funding study row)
    (create-organisation-involvement study row)
    (create-study-status study row)
    (create-study-assignment study row)
    (create-recruitment study row)
    )
  )

(defn post-process-people
  [raw-csv-data]
  (doseq [row raw-csv-data]
    (def title (parse-title (get row :title)))
    (def first-name (get row :first_name))
    (def surname (get row :surname))
    (def role-id (role (get row :role_id)))
    (def person (first (jdbc/query db-spec ["SELECT * FROM people WHERE title = ? AND first_name = ? AND surname = ? LIMIT 1" title first-name surname])))
    (def research-theme-id (research-theme (get row :primary_theme)))
    (println (str "Person ID: " (:id person)))
    (create-person-theme-associations person research-theme-id)
    (create-person-roles person role-id)
    )
  )

(defn import-csv-file
 [csv-file]
 (def table (get csv-file-to-sql-table csv-file))
 (def raw-csv-data (->> (read-csv csv-file)
                    csv-data->maps))

 (def csv-data (->> raw-csv-data
                    (#(if (= table "studies") (pre-process-studies %) %))
                    (#(if (= table "people") (pre-process-people %) %))
                    (#(if (= table "publications") (pre-process-publications %) %))))

  ; (import-csv table csv-data)

  (if (= table "people")
    (post-process-people raw-csv-data)
    )

  (if (= table "studies")
    (post-process-studies raw-csv-data)
    )
    )

(defn -main
  [csv-file]
  (import-csv-file csv-file))
