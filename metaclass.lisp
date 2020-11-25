
(in-package :cldb)


(defclass cldb-class (standard-class)
  ((table-name :initform nil :initarg :table-name)))

;; can only subclass cldb-superclass
(defmethod ccl:validate-superclass ((class cldb-class) (superclass standard-class))
  ;; (eq superclass (find-class 'cldb-entity))
  t)

;; ... or another cldb class (which is perfectly fine)
(defmethod ccl:validate-superclass ((class cldb-class) (superclass cldb-class))
  t)


(defclass cldb-slot-definition (ccl:standard-slot-definition)
  ((presentation-type :initarg :presentation-type
                      :reader slot-definition-presentation-type
                      :initform t)
   (ccl::allocation :initform :database)))

(defclass cldb-direct-slot-definition (ccl:standard-direct-slot-definition cldb-slot-definition)
  ())

(defclass cldb-effective-slot-definition (ccl:standard-effective-slot-definition cldb-slot-definition)
  ((value-getter :initform nil :accessor slot-definition-value-getter)))


;; by default the ID slot is a normal one and the rest come from the database
(defmethod ccl:direct-slot-definition-class ((class cldb-class) &rest initargs)
  ;; (declare (ignore initargs))
  (if (member (second (member :allocation initargs))
              '(:instance :class))
      (call-next-method)
      (find-class 'cldb-direct-slot-definition)))


(defmethod ccl:effective-slot-definition-class ((class cldb-class) &rest initargs)
  (if (or (member (second (member :allocation initargs))
                  '(:instance :class))
          (member (symbol-name (second (member :name initargs)))
                  '("ID" "BASE-VECTOR" "CHANGED-BLOCKS")
                  :test #'equal))
      (call-next-method)
      (find-class 'cldb-effective-slot-definition)))


;; !!! I think I should do this differently
;; I can just get all the ptypes, which will default to nil, and then remove duplicates and see what I'm left with
;; (see the definition in relations/postgres-class - I'm not sure that is right
(defmethod compute-presentation-type ((def cldb-effective-slot-definition) direct-slots)
  (let ((ptypes (remove-duplicates (mapcar #'slot-definition-presentation-type direct-slots)
                                   :test #'equal)))
    ;; !!! CHECK THIS
    (setf (slot-value def 'presentation-type)
          (if (cdr ptypes)
              `(and ,@ptypes)
              (first ptypes)))))


(defmethod compute-presentation-type ((def t) direct-slots)
  (declare (ignore direct-slots)))


(defmethod ccl:compute-effective-slot-definition ((class cldb-class) name direct-slots)
  (declare (ignore name))
  (let ((def (call-next-method)))
    (compute-presentation-type def direct-slots)
    
    def))


;; now, we need to map over all the superclasses to make ID slots for each of them
;; This, I think, is how I will get the inheritance thing to work again
(defmethod ccl:compute-slots ((class cldb-class))
  (let ((slots (call-next-method)))
    
    (append
     ;; walk up the class hierarchy
     (let ((list nil))
       (labels ((walk (class)
                  (push (make-instance 'ccl:standard-effective-slot-definition
                                       :class class
                                       :initargs (list (intern (symbol-name (class-name class)) :keyword))
                                       :type 'integer
                                       :allocation :instance
                                       :name (class-name class)
                                       )
                        list)
                  (loop for super in (ccl::class-direct-superclasses class)
                       when (typep super 'cldb-class)
                       do (walk super))

                  ))
         (walk class))
       list)
     ;; make some additional slots...
     (list 

           (make-instance 'ccl:standard-effective-slot-definition
                          :class class
                          :initargs '(:base-vector)
                          :type t
                          :allocation :instance
                          :name 'base-vector)

           (make-instance 'ccl:standard-effective-slot-definition
                          :class class
                          :initargs '(:changed-blocks)
                          :type t
                          :allocation :instance
                          :name 'changed-blocks))
     
     
     slots)))





;; I think the slot definition should have associated with it a column from the database
;; that will avoid looking up the column definition in the database each time
;; we will store the INDEX of the column, not the column itself
;; This is because the database will keep getting swapped over each time they are updated, but the column indexes will never change once established

(defmethod slot-column-name ((s symbol))
  (symbol-to-db-name s))

(defmethod slot-column-name ((slot-definition cldb-slot-definition))
  (symbol-to-db-name (ccl:slot-definition-name slot-definition)))

(defmethod class-table-name ((c cldb-class))
  (symbol-to-db-name (or (first (slot-value c 'table-name))
                         (class-name c))))

;; this is broken
#+nil(defmethod db-name ((c cldb-class))
  (symbol-to-db-name (class-table-name c)))


;; these should be cached really
;; Will this be called on compilation? I don't want it to be. It needs to be late bound
(defun slot-column (b c slot)
  (let ((slot-class (ccl::slot-definition-class slot)))
    (find-column  b c
                  (class-table-name slot-class)
                  (slot-column-name slot))))

;; Useful for running tests and perfectly safe
(defmethod clear-slot-column-cache ((class cldb-class))
  (dolist (slot (ccl:class-slots class))
    (when (typep slot 'cldb-effective-slot-definition)
      (setf (slot-definition-value-getter slot) nil))))


;; I should override the setter here to just do nothing OR can you declare slots read only?
;; I wonder if I can cache things here
(defmethod ccl:slot-value-using-class ((class cldb-class) instance (slot-definition cldb-effective-slot-definition))
  (funcall (or (slot-value slot-definition 'value-getter)
               (setf (slot-definition-value-getter slot-definition)
                     (let* ((b (slot-value instance 'base-vector))
                            (c (slot-value instance 'changed-blocks))
                            (col (slot-column b c slot-definition))
                            (id->value (column-slot b c col column.id->value))
                            (slot-type (ccl:slot-definition-type slot-definition))
                            (slot-type-class (when (symbolp slot-type)
                                               (find-class slot-type)))
                            (slot-type-initarg (when slot-type-class
                                                 (intern (symbol-name slot-type) :keyword))))
                       
                       (if (and slot-type-class
                                (typep slot-type-class 'cldb-class))
                           (lambda (b c id)
                             (heap-instance b c slot-type-class
                                            slot-type-initarg
                                            ;; !!! I'm assuming the id won't exceed (1- (ash 1 56))
                                            ;; (which would cause us problems anyway)
                                            (parray-ref b c id->value id)))
                           (lambda (b c id)
                             (pobject->lisp-object b c
                                                   (parray-ref b c id->value id)))))))
           
           (slot-value instance 'base-vector)
           (slot-value instance 'changed-blocks)
           (slot-value instance (class-name (ccl::slot-definition-class slot-definition)))))

;; how do I determine whether the slot is bound? Is it always if we have a valid row?
;; This comes down to the question of null handling. Maybe I need something special for null, though it will depend on data types from the database
;; do I get those? All I need to know is whether it's a binary or not.
(defmethod ccl:slot-boundp-using-class ((class cldb-class) instance (slot-definition cldb-effective-slot-definition))
  (declare (ignore instance))
  ;; !!! FIXME - if the database returns :NULL as the slot value then the answer is no
  t)

;; of course, I am not initially going to do ways of mutating things here
;; indeed, if you want to mutate things it's probably going to be necessary to create a postgres-class instance instead
;; it might be nice to just do change-instance.

;; If I were going to give direct write access here I would need to do these things...


;; !!! We need to see if the value is an instance of this metaclass - I should start handling that...
(defun find-instances (b c class slot value)
  (when (symbolp class)
    (setf class (find-class class)))
  (let ((r nil))
    (map-ids-for-value b c
                       (slot-column b c slot)
                       value
                       (lambda (id)
                         (push (heap-instance b c class :id id) r)))
    r))

;; in fact, we shouldn't use make-instance to get references. We need find-instance...
;; (this is more restricted than postgres-class so far)
#+nil(defun find-instance (b c class slot value)
  (let ((a (find-instances class slot value)))
    (if (= (length a) 1)
            
        (error "Found ~A instances of ~A with ~A = ~A - not just 1"
               class slot value))))



;; TODO
;; - handle creation of subclasses (like underwriter-product subclasseses etc) like the postgres-class does
;;   --- this is one of the major points of this exercise - to get that stuff to run fast)

;; incidentally, instantiating these new classes will be very much quicker than postgres-class ones

;; if this were to be used for querying it needs to be as fast as possible
;; it might be better NOT to use it for querying
#+nil(defmethod map-tuples (f (x cldb-class))
  (map-row-ids (lambda (id)
                 (funcall f (make-instance x :id id)))
               (find-table *database* (class-table-name x))))




;; so, to get an instance by ID we just need to do this...
;; (which is annoying because we won't see initargs
(defun heap-instance (b c class id-slot-name id)
  ;; !!! To initialize subclasses will require more work than this
  ;; we have to check for any subclasses and then walk over them looking for any references to this particular superclass
  ;; we may have to make a computed class in the end. If not then we just need to make sure to set all the id slots
  ;; !!! This won't yet handle computed classes  or multiple inheritence
  ;; MI shouldn't be hard to add
  ;; !!! FIXME
  (when (symbolp class)
    (setf class (find-class class)))
  
  (labels ((create (class identifiers id)
             (or (loop for sub in (ccl:class-direct-subclasses class)
                    for match = (collect-1 b c
                                           (index-lookup b c
                                                         (class-table-name sub)
                                                         (class-table-name class))
                                            id)
                      
                    when match
                    return (create sub (cons (intern (symbol-name (class-name sub))
                                                     :keyword)
                                             (cons match
                                                   identifiers))
                                   match))
                 (apply #'make-instance
                        (append (list class
                                      :base-vector b
                                      :changed-blocks c)
                                identifiers)))))
    
    (let ((identifiers (list id-slot-name id)))
      (create class
              identifiers id))))

(defun simple-heap-instance (b c class id-slot-name id)
  (make-instance class
                 :base-vector b
                 :changed-blocks c
                 id-slot-name id))

(defun get-instance (class &optional id)
  (with-database (b c)
    (heap-instance b c class
                   (intern (symbol-name class) :keyword)
                   id)))

(defun id/instance-id (object class)
  (if (integerp object)
      object
      (slot-value object class)))

;; Here is a version which can be composed in with id-mapper etc
(defun map-heap-instance (class)
  (let ((id-slot-name (intern (symbol-name class) :keyword)))
    (lambda (b c id)
      (lambda (f)
        (funcall f (heap-instance b c class id-slot-name id))))))


