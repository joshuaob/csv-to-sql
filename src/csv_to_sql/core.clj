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

; FIXME: remove underscore
(defn research_programme
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM research_programmes WHERE name = ? LIMIT 1" name])) :id))

; FIXME: remove underscore
(defn research_type
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM research_types WHERE name = ? LIMIT 1" name])) :id))

(defn lead-centre
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM lead_centres WHERE name = ? LIMIT 1" name])) :id))

; FIXME: remove underscore
(defn study_type
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM study_types WHERE name = ? LIMIT 1" name])) :id))

; FIXME: use date directly istead of string date
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

; FIXME: remove underscore
(defn project_impact
  [name]
  (get (first (jdbc/query db-spec ["SELECT id FROM project_impacts WHERE name = ? LIMIT 1" name])) :id))

; FIXME: remove underscore
(defn project_purpose
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

; FIXME: date may not always be in desired format
(defn String->Date
  [date]
  (if (empty? date)
    nil
    (.format
      (java.text.SimpleDateFormat. "yyyy-MM-dd")
      (.parse (java.text.SimpleDateFormat. "dd/MM/yyyy") date)
      )))

; FIXME: remove underscore
; FIXME: date may not always be in desired format
(defn published_on
  [date]
  (String->Date date))

(defn String->Double [str]
  (if (not-empty str)
    (Double/parseDouble str)))

; FIXME: verify a status is unique by study id only
; FIXME: add status missing
(defn create-study-status
  [raw-csv-data]
  (doseq [row raw-csv-data]
    (def study-title (get row :title))
    (def study (first (jdbc/query db-spec ["SELECT * FROM studies WHERE title = ? LIMIT 1" study-title])))
    (def duplicate (first (jdbc/query db-spec ["SELECT * FROM study_statuses WHERE study_id = ? LIMIT 1" (get study :id)])))
    (if (nil? duplicate)
      (if-not (empty? (get row :status))
        (if-not (= "Closed" (study-status (get row :status)))
          (do (println "No duplicate")
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
      (println "Duplicate found"))
    )
  )

; FIXME: verify a funding is unique by study id only
(defn create-organisation-involvement
  [raw-csv-data]
  (doseq [row raw-csv-data]
    (def study-title (get row :title))
    (def lead-centre-id (lead-centre (get row :lead_centre_id)))
    (def link-to-nihr-translational-research-collaboration (yes-no-to-int (get row :link_to_nihr_translational_research_collaboration)))
    (def link-to-crf-cross-cutting-theme (yes-no-to-int (get row :project_link_to_crf_cct)))
    (def link-to-crf-explanation (get row :project_link_to_crf_cct_detail))
    (def study (first (jdbc/query db-spec ["SELECT id FROM studies WHERE title = ? LIMIT 1" study-title])))
    (def duplicate (first (jdbc/query db-spec ["SELECT study_id FROM organisation_involvement WHERE study_id = ? LIMIT 1" (get study :id)])))

    (if (nil? duplicate)
      (do
        (println "No duplicate")
        (jdbc/execute! db-spec ["INSERT INTO organisation_involvement SET
                                          study_id = ?,
                                          lead_centre_id = ?,
                                          link_to_nihr_translational_research_collaboration = ?,
                                          link_to_crf_cross_cutting_theme = ?,
                                          link_to_crf_explanation = ?",
                                          (get study :id),
                                          lead-centre-id,
                                          link-to-nihr-translational-research-collaboration,
                                          link-to-crf-cross-cutting-theme,
                                          link-to-crf-explanation
                                          ]))
      (println "Duplicate found"))
    )
  )

; FIXME: verify a funding is unique by study id only
(defn create-study-funding
  [raw-csv-data]
  (doseq [row raw-csv-data]
    (def study-title (get row :title))
    (def study (first (jdbc/query db-spec ["SELECT id FROM studies WHERE title = ? LIMIT 1" study-title])))
    (def funding-category (first (jdbc/query db-spec ["SELECT id FROM funding_categories WHERE name = ? LIMIT 1" (get row :main_funding_category)])))
    (def funding-category (first (jdbc/query db-spec ["SELECT id FROM funding_categories WHERE name = ? LIMIT 1" (get row :main_funding_category)])))
    (def duplicate (first (jdbc/query db-spec ["SELECT * FROM study_funding WHERE study_id = ? LIMIT 1" (get study :id)])))

    (if (nil? duplicate)
      (do
        (println "No duplicate found")
        (jdbc/execute! db-spec ["INSERT INTO study_funding set
                                  main_funding_source = ?,
                                  funding_category_id = ?,
                                  brc_funding_amount = ?,
                                  study_id = ?"
                                  (get row :main_funding_source),
                                  (get funding-category :id),
                                  (String->Double (get row :main_funding_brc)),
                                  (get study :id)
                                  ])
        )
      (println "Duplicate found")
      )
    )
  )

(defn create-person-roles
  [raw-csv-data]
  (doseq [row raw-csv-data]
    (def title (parse-title (get row :title)))
    (def first-name (get row :first_name))
    (def surname (get row :surname))
    (def role-id (role (get row :role_id)))
    (def person (first (jdbc/query db-spec ["SELECT * FROM people WHERE title = ? AND first_name = ? AND surname = ? LIMIT 1" title first-name surname])))
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
  )

; FIXME: confirm object type format (lower case singular of table name)
(defn create-person-theme-associations
  [raw-csv-data]
  (doseq [row raw-csv-data]
    (def title (parse-title (get row :title)))
    (def first-name (get row :first_name))
    (def surname (get row :surname))
    (def research-theme-id (research-theme (get row :primary_theme)))
    (def person (first (jdbc/query db-spec ["SELECT * FROM people WHERE title = ? AND first_name = ? AND surname = ? LIMIT 1" title first-name surname])))
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
  )

(defn create-study-theme-associations
  [raw-csv-data]
  (doseq [row raw-csv-data]
    (def study-title (get row :title))
    (def research-theme-id (research-theme (get row :research_theme_id)))
    (def study (first (jdbc/query db-spec ["SELECT id FROM studies WHERE title = ? LIMIT 1" study-title])))
    (def duplicate (first (jdbc/query db-spec ["SELECT * FROM theme_associations WHERE object_id = ? AND object_type = ? AND theme_id = ? LIMIT 1" (get study :id), "study", research-theme-id])))

    (if (nil? duplicate)
      (if-not (nil? research-theme-id)
        (do
          (println "No duplicate found")
          (jdbc/execute! db-spec ["INSERT INTO theme_associations SET object_id = ?,
                                                                             object_type = ?,
                                                                             theme_id = ?",
                                                                             (get study :id),
                                                                             "study",
                                                                             research-theme-id
                                                                             ]))
        (println "Skipping import: Study has no research theme"))
      (println "Duplicate found")
      )
    )
  )

(defn create-publication-theme-associations
  [raw-csv-data]
  (doseq [row raw-csv-data]
    (def reference (get row :reference))
    (def research-theme-id (research-theme (get row :research_theme_id)))
    (def publication (first (jdbc/query db-spec ["SELECT id FROM publications WHERE reference = ? LIMIT 1" reference])))
    (def duplicate (first (jdbc/query db-spec ["SELECT * FROM theme_associations WHERE object_id = ? AND object_type = ? AND theme_id = ? LIMIT 1" (get publication :id), "publication", research-theme-id])))

    (if (nil? duplicate)
      (if-not (nil? research-theme-id)
        (do
          (println "No duplicate found")
          (jdbc/execute! db-spec ["INSERT INTO theme_associations SET object_id = ?,
                                                                             object_type = ?,
                                                                             theme_id = ?",
                                                                             (get publication :id),
                                                                             "publication",
                                                                             research-theme-id
                                                                             ]))
        (println "Skipping import: Study has no research theme"))
      (println "Duplicate found")
      )
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
      :crn_portfolio_trial
      :orcid_id
      :status
      :total_external_funding_awarded
      :main_funding_dhsc_or_nihr
      :main_funding_category
      ; :research_programme_id
      ; :research_theme_id
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
          (update csv-record :research_type_id #(research_type %))))
    (map (fn [csv-record]
          (update csv-record :randomised #(yes-no-to-int %))))
    (map (fn [csv-record]
          (update csv-record :first_in_man #(yes-no-to-int %))))
    (map (fn [csv-record]
          (update csv-record :rec_approval_required #(yes-no-to-int %))))
    (map (fn [csv-record]
          (update csv-record :study_type_id #(study_type %))))
    (map (fn [csv-record]
          (update csv-record :primary_intervention_id #(intervention %))))
    (map (fn [csv-record]
          (update csv-record :project_impact_id #(project_impact %))))
    (map (fn [csv-record]
          (update csv-record :project_purpose_id #(project_purpose %))))
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
           (update csv-record :published_on #(published_on %))))
    (map (fn [csv-record]
           (update csv-record :nihr_acknowledgement #(yes-no-to-int %))))
    (map (fn [csv-record]
           (update csv-record :open_access #(yes-no-to-int %))))
    ))

(defn post-process-studies
  [raw-csv-data]
  (create-study-funding raw-csv-data)
  (create-organisation-involvement raw-csv-data)
  (create-study-status raw-csv-data))

(defn post-process-people
  [raw-csv-data]
  (create-person-theme-associations raw-csv-data)
  (create-person-roles raw-csv-data))

(defn import-csv-file
 [csv-file]
 (def table (get csv-file-to-sql-table csv-file))
 (def raw-csv-data (->> (read-csv csv-file)
                    csv-data->maps))

 (def csv-data (->> raw-csv-data
                    (#(if (= table "studies") (pre-process-studies %) %))
                    (#(if (= table "people") (pre-process-people %) %))
                    (#(if (= table "publications") (pre-process-publications %) %))))

  (import-csv table csv-data)

  (if (= table "people")
    (post-process-people raw-csv-data)
    )

  (if (= table "studies")
    (post-process-studies raw-csv-data)))

(defn -main
  [csv-file]
  (import-csv-file csv-file))
