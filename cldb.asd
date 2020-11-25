
(asdf:defsystem :cldb
  :description "Common Lisp Database"
  :author "VIP"
  :serial t
  :depends-on ( ;; "vip-utils"
               "vip-clim-core" "trivial-utf-8" "anaphors" "clim-web")
  
  :components ((:file "package")
               ;; (:file "iterators")
               (:file "persistent-heap")
               (:file "database")
               (:file "query")
               (:file "metaclass")
               (:file "presentations")
               ))
