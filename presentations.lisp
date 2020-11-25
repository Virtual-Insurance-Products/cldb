
(in-package :cldb)

;; This allows any cldb-class instance to be presented to a string w/o needing a common superclass or implementing custom methods
(define-presentation-method present :around ((object t) (type t) (stream stream) (view textual-view) &key acceptably)
  type acceptably ; ignore
  (if (typep (class-of object) 'cldb:cldb-class)
      ;; (it would be good to put the name in like for postgres-class things)
      (format stream "~A ~A"
              (string-capitalize (cl-ppcre:regex-replace-all "-" (format nil "~A" (class-name (class-of object))) " "))
              (slot-value object (type-of object)))
      (call-next-method)))

(define-presentation-method accept :around ((x t) (stream (eql :simple-parser))
                                                  view
                                                  &key
                                                  error-if-not-eof)
  error-if-not-eof
  view
  nil
  (with-presentation-type-decoded (name)
      x
    (if (awhen (find-class name nil)
          (typep it 'cldb::cldb-class))
        (let ((id (or
                   (when (simple-parser:sp-scan (format nil "^~A" (cl-ppcre:regex-replace-all "-" (symbol-name name) " "))
                                                :ignore-case t)
                     (simple-parser:sp-skip-whitespace)
                     (or (parse-integer (simple-parser:sp-scan "^\\d+"))
                         (simple-parser:sp-error "Expected ID here")))
                   (parse-integer (simple-parser:sp-scan "^\\d+")))))
          (when id 
            (simple-parser:sp-skip-whitespace)
            (let ((object (cldb:get-instance name id)))
              ;; !!! Should this be added in for the rel:acceptable-object-with-id as well?
              ;; (the optional bit)
              (simple-parser:sp-scan "^\\s*(?:<[^>]*>)?")
              object)))

        (call-next-method))))

(define-presentation-type cqf (&optional (?result-type t))
  :inherit-from 'function)

(define-presentation-method presentation-typep (object (type cqf))
  (functionp object))

(define-presentation-method type-for-subtype-tests ((type cqf))
  (with-presentation-type-decoded (name)
      type
    name))

(define-presentation-method presentation-subtypep ((type cqf) putative-supertype)
  (with-presentation-type-decoded (putative-name putative-parameters)
      putative-supertype
    (and (eq putative-name 'cqf)
         (presentation-subtypep ?result-type (first putative-parameters))
         (values t t))))


