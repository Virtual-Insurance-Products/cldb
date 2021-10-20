
(in-package :cldb)


;; build a new database on top of the persistent heap.
;; This means we don't need to implement any copying in here at all - just mutate things.



;; so, I need something to access the columns in the database
;; the columns will have: name, table and the data
;; the data will be in the form of an id->value index (an array) and, optionally, a value->id-list index

;; I don't know whether to make the initial id->value index a flat array (marginally best for performance)

;; alternatively, I could flatten things by allowing a collection of columns 


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; DATABASE LIFE CYCLE

;; this will point to the header information
(defconstant +database-header-length+ 20)
(defconstant +database-header+ (make-pointer p/array-t-16
                                             1 ; address of the header in the mapped heap
                                             (1- +database-header-length+)))

(defmacro enumerate (&rest r)
  `(progn
     ,@ (loop for i from 0
           for c in r
           collect `(defconstant ,c ,i))))

(enumerate
    h/string-intern-table
  h/column-table
  h/version
  h/creation-date
  h/modification-date
  h/info
  h/transaction-id
  )



(defun make-database ()
  (with-new-writable-heap (w #(0))
    (let ((header (pmake-array w +database-header-length+)))
      (unless (= header +database-header+)
        (error "Something has gone wrong"))
      
      (loop for (index value)
         on (list
             h/string-intern-table  (make-hash-table-precursor w) ; 8MB for the string intern table
             h/column-table         p/nil ; no columns yet...
             h/version              1
             h/creation-date        (get-universal-time)
             h/modification-date    (get-universal-time)
             h/info                 (pstring nil "CLDB") ; string which fits in pointer doesn't need interning OR writing to heap
             h/transaction-id       0
             )
         by #'cddr
         do (setf (parray-ref w header index) value))
      
      (with-heap-snapshot (b c w top)
        (make-file-heap :base-vector b
                        :changed-blocks c
                        :top top
                        :mapped-vector nil)))))


;; (snapshot-database "/Users/david/Desktop/new-database" (make-database))


;; from now on we want to be able to have an open database in a dynamic variable in read mode and have a process to change it and then commit
;; so I'm going to need a way to establish dynamic bindings for the database from which to read
;; then I need this...

(defvar *current-database* nil)

;; this is a thin wrapper around open-heap which sets it in this variable...
;; maybe I could make the variable somehow more differenter
(defun open-database (file)
  (setf *current-database*
        (open-heap file))
  :ok)

;; close the database by unmapping the file
(defun close-database ()
  (when (file-heap-mapped-vector *current-database*)
    (close-mapped-file *current-database*))
  (setf *current-database* nil))

;; ... things to find the latest snapshot etc. I guess I *could* have a database listing the latest snapshot or something. I ought to make database replacement atomic.

(defmacro with-database ((base changed &optional top (database '*current-database*)) &body forms)
  (let ((d (gensym "database")))
    `(let* ((,d ,database)
            (,base (file-heap-base-vector ,d))
            (,changed (file-heap-changed-blocks ,d))
            ,@ (when top
                 `((,top (file-heap-top ,d)))))
       ,@forms)))

;; (with-database (b c) (+ 1 2))
;; (with-database (b c x) (+ 1 2))

;; the problem here is how to read and write at the same time?
;; I might need to slightly refine some of these things, but I think the design is basically good
;; I want to keep reading and writing very much separated
;; writes will only happen in one thread which is responsible for committing transactions from Pg initially.
(defmacro with-database-transaction ((w &optional (database '*current-database*) (save-mtime t)) &body body)
  (let ((b (gensym "base"))
        (c (gensym "changed"))
        (top (gensym "top")))
    `(with-database (,b ,c ,top ,database)
       (let ((,w (writable-heap ,b ,c ,top)))
         (let ((result (progn ,@body)))
           ;; then assuming there was no error we will commit the transaction...
           ,(when save-mtime
              `(setf (parray-ref ,w +database-header+ h/modification-date) (get-universal-time)))
           ;; There's no need to use with-heap-snapshot, which will copy the changed-blocks array again
           ;; !!! We must NOT pass the w outside the with-database-transaction
           (setf ,database
                 (make-file-heap :base-vector (pheap-base-vector ,w)
                                 ;; !!! Copy it to make a non adjustable array
                                 ;; this is partly to work around a CCL bug in replace wrt adjustable arrays
                                 :changed-blocks (copy-seq (pheap-changed-blocks ,w))
                                 :top (pheap-top ,w)
                                 :mapped-vector (file-heap-mapped-vector ,database)))
           ;; I'll clear out the things which I'm referencing in the file-heap
           (setf (pheap-base-vector ,w) nil
                 (pheap-changed-blocks ,w) nil)
           result)))))



;; and then snapshot the database...
(defun snapshot-database (file &optional (database *current-database*) (replace :error))
  ;; This is done in a transaction because saving a heap may assign some +empty-block+ entries to the changed-blocks array
  ;; this is done to speed up successive saves. SO, because save-heap expects a pheap, and because of the +empty-blocks+ thing...
  (with-database-transaction (w database nil)
    (save-heap w file :replace replace)))

;; (snapshot-database "/Users/david/Desktop/db2" (make-database))
;; (snapshot-database "/Volumes/tank/temp/db2" (make-database))
;; (with-database-transaction (w) (+ 1 2))



;; Whatever our representation of a column is going to be it will have to not change.
;; I'm thinking, therefore, of just storing the index into the column table, which will be the first thing in the loaded heap.


;; NEXT UP...

;; - dump in some data and see how we get on
;; - save it and try loading it
;; - do the metaclass again. How does that work? It needs to do a with-database. Could happen at instance creation time if that helps.
;; - write some things to query again. 



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; DATABASE DATA STRUCTURES...

;; Let's intern strings...

;; we need to: find an interned string if it exists
;; create an interned string if none exists (won't check whether it does)

(defun string-intern-table (b c)
  (parray-ref b c +database-header+ h/string-intern-table))

(defun find-unique-string (b c string)
  (let ((vec (trivial-utf-8:string-to-utf-8-bytes string)))
    (if (< (length vec) 6)
        (pstring nil string vec) ; nil as writable heap - we don't want to write here
        (let* ((list (hash-table-list b c
                                      (string-intern-table b c)
                                      string)))
          ;; loop over all the interned strings trying to find one which matches
          ;; each string is stored as a list of bytes
          (with-pobject-list-iterator (b c list more next)
            (let ((vec2 (make-array (length vec) :element-type '(unsigned-byte 8))))
              (loop while more
                 for string = next
                 when (and (= (length vec)
                              (plength b c string))
                           (progn
                             (read-byte-vector b c vec2 (address-field string))
                             (equalp vec vec2)))
                 return string)))))))

;; now this is interesting, because this needs b and c to read from as well
;; BUT that's ok - w contains them.
;; I should probably generalize the hash table precursors more...
(defun add-unique-string (w string)
  (let ((vec (trivial-utf-8:string-to-utf-8-bytes string))
        (b (pheap-base-vector w))
        (c (pheap-changed-blocks w)))
    (if (< (length vec) 6)
        (error "String does not need adding to string table - is short")
        (let ((pstring (pstring w string)))
          (add-to-hash-table w b c (string-intern-table b c)
                             string pstring)
          pstring))))

;; (find-unique-string nil nil "David")

(defun intern-string (w string)
  (or (find-unique-string (pheap-base-vector w)
                          (pheap-changed-blocks w)
                          string)
      (add-unique-string w string)))





;; now let's try and get to columns
;; the id->value map will just be a pointer to a (hopefully flat) array.
;; I will then, somehow, need to acces the object referenced, which will either be a number of some type or a string
;; in fact, the general pobject->lisp-object function will handle that. Good...

;; Then I need to be able to set the column values.
;; For a column I need: a string for its name, a string for it table, and id list and (optionally) a value->id mapping

(defclass column (boxed-pheap-object)
  ;; this points to the column's info array in memory
  ((tag :initform p/array-t-16 :reader tag)))




(enumerate
    column.name column.table column.id->value column.id-overflow column.index
    column.fill-pointer)


;; !!! Should these 2 be changed to just take the pointer rather than the CLOS instance?
;; The issue is partly to do with lack of CLOS instances in the heap. Maybe I should add them somehow, but I probably won't need it
;; w/o CLOS instances or structs or something it means these columns are weakly typed
(defun (setf column-slot) (value w column slot)
  (setf (parray-ref w (pointer column) slot) value))

(defun column-slot (b c column slot)
  (parray-ref b c (pointer column) slot))

(defmethod name ((column column))
  (pobject->lisp-object
   (base-vector column)
   (changed-blocks column)
   (column-slot (base-vector column)
                (changed-blocks column)
                column
                column.name)))

;; REPETETIVE
(defmethod table ((column column))
  (pobject->lisp-object
   (base-vector column)
   (changed-blocks column)
   (column-slot (base-vector column)
                (changed-blocks column)
                column
                column.table)))

(defmethod used-rows (b c (x column))
  (pobject->lisp-object b c (column-slot b c x column.fill-pointer)))

(defmethod base-rows (b c (x column))
  (plength b c (column-slot b c x column.id->value)))

(defmethod print-object ((x column) (s stream))
  (print-unreadable-object (x s :type 'column)
    (let* ((b (base-vector x))
           (c (changed-blocks x))
           (fraction (/ (used-rows b c x)
                        (base-rows b c x))))
      (format s "(~$%)~A    ~A.~A (~A base rows, ~A used rows, ~A)"
              (* 100 fraction)
              (if (> fraction 7/10)
                  " WARNING - >70% in use " "")
              
              (table x)
              (name x)
              (base-rows b c x)
              (used-rows b c x)
              (if (= (column-slot b c x column.index) 0)
                  "no index" "indexed")))))



;; This initially makes a column of the given size which is a flat array
;; this will make lookup as fast as can be (pretty fast!)
(defun make-simple-column (w initial-size table name &key (index-p t))
  (let ((name (intern-string w name))
        (table (intern-string w table)))

    ;; create the column info slots...
    (let ((col (make-instance 'column :pointer (pmake-array w 10)
                              :base-vector (pheap-base-vector w)
                              :changed-blocks (pheap-changed-blocks w))))
      (loop for (slot value) on
           (list column.name name
                 column.table table
                 column.id->value (pmake-array w initial-size)
                 ;; overflow is used if we exceed the initial
                 ;; allocation. It's a 2-level tree. It will be
                 ;; slightly slower to dereference since we have to
                 ;; map to the block and then map to the element OF
                 ;; the block.  Ideally one should try and allocate
                 ;; big enough columns. I *could* re-allocate columns,
                 ;; but it would leave garbage.  Snapshot time and
                 ;; size will become an issue eventually.  It might be
                 ;; nice if I could sparse allocate things so I don't
                 ;; have to write zero blocks.  Maybe if I just seek
                 ;; through the file when I have zero ones...
                 column.id-overflow p/nil                     
                 column.fill-pointer (lisp-object->pobject 0))
           
         by #'cddr
         do (setf (column-slot w col slot) value))
      
      (when index-p
        (setf (column-slot w col column.index) (make-hash-table-precursor w)))

      (ppush w (pheap-base-vector w)
             (pheap-changed-blocks w)

             (pointer col) (parray-ref +database-header+ h/column-table))
      
      col)))

(defun all-columns (b c)
  (with-pobject-list-iterator (b c (parray-ref b c +database-header+ h/column-table)
                                 more next)
    (loop while more
       for col = next
       collect (make-instance 'column :pointer col
                              :base-vector b :changed-blocks c))))

;; (with-database (b c) (all-columns b c))

(defun table-columns (b c table)
  (remove table
          (cldb::all-columns b c)
          :test-not #'equal
          :key #'cldb::table))


;; shall I make a representation of a table here?
(defun all-tables (b c)
  (remove-duplicates (mapcar #'table (all-columns b c))
                     :test #'equal))

;; (with-database (b c) (all-tables b c))

;; The following will be somewhat less efficient
#+nil(find-if (lambda (col)
                (and (equal (name col) "name") (equal (table col) "brand")))
              (with-database (b c) (all-columns b c)))

(define-condition missing-cldb-column (error)
  ((table-name :initarg :table-name :reader table-name)
   (column-name :initarg :column-name :reader column-name)))

(defmethod print-object ((x missing-cldb-column) (s stream))
  (format s "Missing CLDB Column ~A.~A" (table-name x) (column-name x)))

;; (error 'missing-cldb-column :table-name "foo" :column-name "bar")

;; the following could be implemented trivially in terms of the above. I might do that. Do I need this to be fast?
(defun find-column (b c table name &optional (error-if-not-found t))
  (let ((ptable (find-unique-string b c table))
        (pname (find-unique-string b c name)))
    (or
     (when (and ptable pname)
       (with-pobject-list-iterator (b c (parray-ref b c +database-header+ h/column-table)
                                      more next)
         (loop while more
            for col = next
            when (and (= (parray-ref b c col column.name) pname)
                      (= (parray-ref b c col column.table) ptable))
            return (make-instance 'column :pointer col
                                  :base-vector b :changed-blocks c))))
     
     (when error-if-not-found
       (error 'missing-cldb-column :table-name table :column-name name)))))





;; I would like a direct pointer to the id->value array for a column so I can just take its address
;; and dereference it and length check it to get the value. That will be swell.
;; What if I exceed the number of elements? Maybe I can then put in an overflow thing in the column structure
;; That will link to a 2-level such map
;; that way, as long as I *don't* overflow I can be as fast as possible

;; now, I need to start making setters and getters for column values
;; these will be very simple really...

;; when implementing the metaclass I can just use parray-ref directly
;; instances will probably have to keep a reference to b and c in order to get slot values
;; however I create them (a wrapper around make-instance perhaps, or something else) will handle this

;; If I have the function with this prototype then I don't need any lookup for those fields
;; those fields are simple slot accesses...
(declaim (inline column-value*))
(defun column-value* (b c id->value overflow id)
  (declare (optimize (speed 3))
           (type fixnum id->value id))
  (if (< id (plength b c id->value))
      (parray-ref* b c id->value id)    ; skip bounds check
      ;; if the MOP hasn't retrieved the overflow slot (maybe it wasn't overflowing) then we will be given a function
      ;; to fault it in
      (if (functionp overflow)
          (funcall overflow id)
          (error "Must fault over to overflow"))))


;; !!! Will it be more efficient to turn this into a macro? 
;; maybe I could even provide custom versions depending on the array type
;; by taking maximum advantage of what is known at the time the metaclass is first instantiated I can probably get this very fast
(defun column-value (b c col id)
  (pobject->lisp-object
   b c (column-value* b c
                      (column-slot b c col column.id->value)
                      (column-slot b c col column.id-overflow) id)))





(defun get-database-object (b c lisp-object)
  (if (stringp lisp-object)
      (find-unique-string b c lisp-object)
      ;; !!! I think the following needs retrieving
      (lisp-object->pobject lisp-object)))



;; does the following make any difference?
(declaim (inline set-column-value))
(defun set-column-value (w b c id->value overflow index id value base-length)
  (declare (ignore overflow))
  (declare (optimize (speed 3))
           (type fixnum id->value index id))
  ;; !!! I might move this logic into the higher level function too
  (let ((pobject (if (stringp value)
                     (intern-string w value)
                     (lisp-object->pobject value))))

    ;; NO-OP if it's unchanged
    (unless (= (parray-ref* b c id->value id)
               pobject)
      ;; Don't index null values (which are used to delete rows)
      (when (and index (not (= pobject p/null)) (not (= index 0)))
        ;; For the moment I'm just going to define this to not necessarily map a list w/o duplicates
        ;; (add-to-hash-table w b c index value id)
        (add-to-sorted-hash-table w b c index value id))

      (if (< id base-length)
          (set-array-ref* w id->value id pobject)
          (error "Handle overflow")))))



;; This assumes a normal lisp value
(defun (setf column-value) (value w col id &optional id->value)
  (let ((b (pheap-base-vector w))
        (c (pheap-changed-blocks w)))

    (unless id->value
      (setf id->value
            (column-slot b c col
                         column.id->value)))

    (when (> id (pobject->lisp-object b c (column-slot b c col column.fill-pointer)))
      (setf (column-slot w col column.fill-pointer) (lisp-object->pobject id)))
    
    (set-column-value w b c
                      id->value
                      (column-slot b c col column.id-overflow)
                      (column-slot b c col column.index)
                      id
                      value
                      (plength b c id->value))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; MODIFICATIONS

;; They won't start off as this. I don't delete things much really.
(defun delete-row (w table id)
  (let ((b (pheap-base-vector w))
        (c (pheap-changed-blocks w)))
    (loop for col in (table-columns b c table)
       do
         (setf (column-value w col id) :NULL))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; INDEX LOOKUP

;; The following function is just a basic, non combinable mapping function using the column's index
(defun map-ids-for-value (b c column value f)
  "Map the function f over every id where the specified column has the specified value by using the value->ids index"
  (let ((test-pobject (get-database-object b c value))
        (id->value (column-slot b c column column.id->value))
        (overflow (column-slot b c column column.id-overflow)))
    (when test-pobject
      (with-pobject-list-iterator (b c (hash-table-list b c
                                                        (column-slot b c column column.index)
                                                        value)
                                     more next)
        (loop while more
           for id = next
           for column-value = (column-value* b c id->value overflow id)
           when (= column-value test-pobject)
           do
             (funcall f id))))))





;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; COMPOSABLE QUERYING FUNCTIONS (CQFs)

(defun symbol-to-db-name (s)
  (substitute #\_ #\-
              (string-downcase
               (symbol-name s))))

(defmethod db-name ((s string)) s)
(defmethod db-name ((s symbol)) (symbol-to-db-name s))


;; Now, in order to define a set of functions which can be composed
;; together with a special composition function in order to run
;; queries WITHOUT needing to do column lookup &c in the inner loops,
;; I will define a function type with the following general form:-
#|
(defun satisfies (test)
  (lambda (value)
    (lambda (f)
      (when (funcall test value)
        (funcall f value)))))
|#

;; ... which is equivalent to the following:-
(defun satisfies (test)
  "Returns a CQF which checks whether its input satisfied 'test' (a predicate function)."
  (lambda (b c value)
    (declare (ignore b c))
    (if (funcall test value)
        (lambda (f) (funcall f value))
        (constantly nil))))

;; This probably just sort of lifts a function or something
(defun applied (f)
  "Returns a CQF which applies f (a function) to each value."
  (lambda (b c value)
    (declare (ignore b c))
    (lambda (g)
      (funcall g (funcall f value)))))

;; take the product of 2 CQFs...
(defun product (x y)
  "Returns a CQF of x and y, both CQFs, representing the cartesian product of x and y."
  (lambda (b c in)
    (lambda (f)
      (funcall (funcall x b c in)
               (lambda (value1)
                 (funcall (funcall y b c in)
                          (lambda (value2)
                            (funcall f (cons value1 value2)))))))))

;; Generate a fixed list
(defun cqf-const (list/atom)
  "Returns a CQF which generates a constant list"
  (lambda (b c in)
    (declare (ignore b c in))
    (if (listp list/atom)
        (lambda (f)
          (mapcar f list/atom))
        (lambda (f)
          (funcall f list/atom)))))

(defun cqf-union (&rest fns)
  "Returns a CQF which generates the union of all the fns"
  (lambda (b c in)
    (lambda (f)
      (mapcar (lambda (x)
                (funcall (funcall x b c in)
                         f))
              fns))))

;; (collect nil nil (cqf-union (cqf-const '(1 2 3)) (cqf-const '(a b c))) nil)

;; The function f might be called many times:-
(defun naturals ()
  "Returns a CQF to generate natural numbers starting from its input value."
  (lambda (b c start)
    (declare (ignore b c)) ; doesn't require reference to the database
    (lambda (f)
      (loop for i from start
         do (funcall f i)))))

;; We can then compose such functions together as follows:-
(defun compose (&rest r)
  "Compose CQFs"
  (flet ((comp (a b)
           (lambda (base changed value)
             (lambda (function)
               (funcall (funcall a base changed value)
                        (lambda (id)
                          (funcall (funcall b base changed id)
                                   function)))))))
    (reduce #'comp r)))

;; Then, if we want to collect values we can do so as follows:-
(defun collect (b c cqf &optional value limit)
  "Collect the values in a list from a given CQF function when applied to an input value"
  (let ((r nil)
        (head (cons nil nil))
        (count 0))
    (block iter
      (funcall (funcall cqf b c value)
               (lambda (id)
                 (incf count)
                 (when (and limit (> count limit))
                   (return-from iter nil))
                 (if r
                     (setf (cdr head) (cons id nil)
                           head (cdr head))
                     (setf r head
                           (car head) id)))))
    r))


(defun collect-1 (b c cqf value)
  "Get 1 value from a CQF"
  (funcall (funcall cqf b c value)
           (lambda (x)
             (return-from collect-1 x))))

;; just in case we get passed a nil in - this just checks for existence
(defun exists (b c cqf value)
  (funcall (funcall cqf b c value)
           (lambda (x)
             (declare (ignore x))
             (return-from exists t))))

(defun collect-unique (b c cqf value)
  "Get the single, unique value from a CQF and error if it yields >1 value"
  (let ((r (collect b c cqf value 2)))
    (when (cdr r)
      (error "Not unique"))
    (first r)))

(defun cqf-count (b c cqf value)
  (let ((count 0))
    (funcall (funcall cqf b c value)
             (lambda (x)
               (declare (ignore x))
               (incf count)))
    count))

(defun reduced (b c cqf value reducer &optional initial)
  (let ((got-one nil)
        (current initial))
    (funcall (funcall cqf b c value)
             (lambda (x)
               (setf current
                     (if got-one
                         (funcall reducer x current)
                         x))))
    current))

;; Given the above we can then collect, for example, 10 odd natural numbers as follows:-
;; (collect nil nil (compose (naturals) (satisfies #'oddp)) 1 10)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; DATABASE QUERY CQFs


;; This is fully curried so that it can be compose using a special composition operator
;; if I wanted to do this w/o the function call overhead I would have to use macrology to get rid of them
;; I *could* do that, but it's harder to get right
(defun column-index-lookup (column)
  "Returns a CQF which does index lookup using the value->ids index for the given column"
  (let* ((b (slot-value column 'base-vector))
         (c (slot-value column 'changed-blocks))
         (id->value (column-slot b c column column.id->value))
         (overflow (column-slot b c column column.id->value))
         (index (column-slot b c column column.index)))
    (lambda (b c value)
      (let ((test-pobject (get-database-object b c value)))
        (if test-pobject
            (lambda (f)
              (with-pobject-list-iterator (b c (hash-table-list b c
                                                                index
                                                                value)
                                             more next)
                (loop while more
                   for id = next
                   for column-value = (column-value* b c id->value overflow id)
                   when (= column-value test-pobject)
                   do
                     (funcall f id))))
            (lambda (f)
              (declare (ignore f))))))))

(defun index-lookup (b c table column)
  "Returns a CQF which does index lookup using the value->ids index for the specified column"
  (column-index-lookup (find-column b c (db-name table)
                                    (db-name column))))

;; use this to narrow to rows with a specific value for some column
;; use map-ids initially
(defun column-equal (b c table column value &key not)
  "Returns a CQF which does equality testing for the specified column's value"
  (let* ((column (find-column b c (db-name table) (db-name column)))
         (id->value (column-slot b c column column.id->value))
         (overflow (column-slot b c column column.id-overflow)))
    (lambda (b c id)
      (let ((test-pobject (get-database-object b c value)))
        (if test-pobject
            (if not
                (lambda (f)
                  (unless (= test-pobject
                             (column-value* b c id->value overflow id))
                    (funcall f id)))
                (lambda (f)
                  (when (= test-pobject
                           (column-value* b c id->value overflow id))
                    (funcall f id))))
            (lambda (f)
              (declare (ignore f))))))))


;; I guess column-satisfies could be composed using in, get-column and satisfies
(defun column-satisfies (b c table column test)
  "Returns a CQF which checks that the specified column's value satisfies 'test' (a predicate function)"
  (let* ((column (find-column b c (db-name table) (db-name column)))
         (id->value (column-slot b c column column.id->value))
         (overflow (column-slot b c column column.id-overflow)))
    (lambda (b c id)
      (lambda (f)
        (when (funcall test (pobject->lisp-object b c (column-value* b c id->value overflow id)))
          (funcall f id))))))

;; I guess I could put in sorts too

;; get a column value
(defun get-column-value (b c table column)
  "Returns a CQF to get the value of the specified column"
  (let* ((column (find-column b c (db-name table) (db-name column)))
         (id->value (column-slot b c column column.id->value))
         (overflow (column-slot b c column column.id-overflow)))
    (lambda (b c id)
      (lambda (f)
        (let ((object (pobject->lisp-object b c (column-value* b c id->value overflow id))))
          (unless (eq object :null)
            (funcall f object)))))))



;; getting only unique values will require either assuming sortedness OR getting all the values and then uniquing them
;; (defun unique (&optional (test #'equal)))

(defun all-column-values (b c table &key (include-id t) (include-names t))
  (let* ((columns (reverse (remove table
                                   (all-columns b c)
                                   :key #'table
                                   :test-not #'equal)))
         (id->value (mapcar (lambda (column)
                              (column-slot b c column column.id->value))
                            columns))
         (overflow (mapcar (lambda (column)
                             (column-slot b c column column.id-overflow))
                           columns)))
    (lambda (b c id)
      (lambda (f)
        (let ((values (mapcar (lambda (id->value overflow)
                                (pobject->lisp-object b c (column-value* b c id->value overflow id)))
                              id->value
                              overflow)))
          (when (find :null values :test-not #'eq)
            (let ((v (if include-names
                         (loop for v in values
                            for c in columns
                            collect (cons (name c) v))
                         values)))
              (funcall f (if include-id
                             (cons (cons "id" id)
                                   v)
                             v)))))))))


;; This must be used as the starting point really - going from a column value to the name of a table won't likely be useful
;; alternatively, the table could be passed in at the start
(defun table-rows ()
  (lambda (b c table)
    (let ((max (reduce #'max
                       (mapcar (lambda (col)
                                 (column-slot b c col column.fill-pointer))
                               (remove table
                                       (all-columns b c)
                                       :key #'table
                                       :test-not #'equal)))))
      (lambda (f)
        (loop for i from 1 to max
             ;; !!! Should I try and eliminate nulls here? I would have to check all the columns
             do (funcall f i))))))



(defun in (subquery)
  "Returns a CQF which checks that the incoming value is in 'subquery', which must be a CQF"
  (lambda (b c id)
    (lambda (f)
      (when (block subq
              (funcall (funcall subquery b c id)
                       (lambda (x)
                         (declare (ignore x))
                         (return-from subq t))))
        (funcall f id)))))

(defun not-in (subquery)
  "Returns a CQF which checks that the incoming value is NOT in 'subquery', a CQF"
  (lambda (b c id)
    (lambda (f)
      (unless (block subq
                (funcall (funcall subquery b c id)
                         (lambda (x)
                           (declare (ignore x))
                           (return-from subq t))))
        (funcall f id)))))

(defun cldb-foreign-key-slot-p (class slot)
  (let* ((slot-type (ccl:slot-definition-type
                     (find slot (ccl:class-slots (if (symbolp class)
                                                     (find-class class) class))
                           :key 'ccl:slot-definition-name)))
         (slot-class (awhen (symbolp slot-type)
                       (find-class slot-type nil))))
    
    (typep slot-class 'cldb-class)))

;; CLOS query methods
(defun lookup (b c class slot)
  (let ((lookup (compose (index-lookup b c (class-table-name (find-class class))
                                       (db-name slot))
                         (map-heap-instance class))))
    (if (cldb-foreign-key-slot-p class slot)
        (compose (applied (lambda (x)
                            (id/instance-id x slot)))
                 lookup)
        lookup)))



;; Of course, I should re-use the slot-value method...
(defun get-slot (class slot)
  (let ((class-id-symbol (intern (symbol-name class) :keyword)))
    (lambda (b c object)
      (lambda (f)
        (funcall f
                 (slot-value (if (integerp object)
                                 (heap-instance b c class class-id-symbol object)
                                 object)
                             slot))))))

(defun slot-equal (slot value)
  (lambda (b c object)
    (declare (ignore b c))
    (lambda (f)
      (when (equal value
                   (slot-value object slot))
        (funcall f object)))))

(defun slot-satisfies (slot test)
  (lambda (b c object)
    (declare (ignore b c))
    (lambda (f)
      (when (funcall test (slot-value object slot))
        (funcall f object)))))
