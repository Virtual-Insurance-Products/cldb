
(in-package :cldb)


(defclass query-node-class (standard-class)
  ((query-node-matches :initarg :query-node-matches :reader query-node-matches :initform nil)))

(defmethod ccl:validate-superclass ((class query-node-class) (superclass standard-class))
  t)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; These are some descriptions of types which are going to be propagated through the pipeline

(defclass cqf-value-type () ())

(defclass cqf-object-reference-type (cqf-value-type)
  ((object-class :reader object-class :initarg :object-class)))

(defclass cqf-object-type (cqf-object-reference-type) ())

(defclass cqf-object-id-type (cqf-object-reference-type) ())


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Then we construct CQF-node classes representing syntax nodes


;; (setf (find-class 'cqf-node) nil)
(defclass cqf-node ()
  ((functor :initarg :functor :reader functor)
   (parameters :initarg :parameters :accessor parameters)
   (input-type :initform (make-instance 'cqf-value-type)
               :accessor input-type
               :initarg :input-type))
  (:documentation "CQF Syntax Node superclass")
  (:metaclass query-node-class)
  ;; superclass doesn't match anything
  (:query-node-matches))


(defmethod handles-functor ((x query-node-class) functor)
  (or (eq (class-name x) functor)
      (find functor (query-node-matches x))
      (find '* (query-node-matches x))))

(defmethod priority ((x query-node-class))
  (if (equal (query-node-matches x) '(*))
      0 1))


(defun walk-query-node-classes (f)
  (labels ((walk (class)
             (funcall f class)
             (mapcar #'walk
                     (ccl:class-direct-subclasses class))))
    (walk (find-class 'cqf-node))))

(defun find-applicable-classes (functor)
  (let ((results nil))
    (walk-query-node-classes (lambda (class)
                               (when (handles-functor class functor)
                                 (push class results))))
    (sort results '> :key #'priority)))

;; (find-applicable-classes 'index-lookup)

(defun most-applicable-class (functor)
  (first (find-applicable-classes functor)))

;; !!! This should automatically find the applicable class
(defun make-cqf-node (functor params &rest more)
  (apply #'make-instance
         `(,(most-applicable-class functor)
            :functor ,functor
            :parameters ,params
            ,@more)))


;; then we can define how to make a query node from an expression
(defun make-query-node-for-expression (expression)
  (make-cqf-node (first expression)
                 (cdr expression)))


;; (make-query-node-for-expression '(satisfies #'oddp))
;; (make-query-node-for-expression '(foo 12 #'oddp))



(defclass database-cqf-node (cqf-node)
  ((base-vector :accessor base-vector :initarg :base-vector)
   (changed-blocks :accessor changed-blocks :initarg :changed-blocks))
  (:metaclass query-node-class)
  (:query-node-matches)
  (:documentation "CQF node which takes base-vector and changed-blocks parameters"))


;; handles anything which isn't a more concrete subclass
(defclass general-cqf (cqf-node) ()
  (:documentation "CQF syntax node used for all general non database ones")
  (:metaclass query-node-class)
  (:query-node-matches *))



(defclass general-database-cqf (general-cqf database-cqf-node) ()
  (:documentation "CQF Syntax node for general database CQF calls")
  (:query-node-matches get-column-value all-column-values)
  
  (:metaclass query-node-class))

(defclass input-pass-through-cqf (general-cqf) ()
  (:documentation "CQF Syntax node which passes the input object along")
  (:metaclass query-node-class))


(defclass clos-cqf-node ()
  ())

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; CONCRETE NODE TYPES

(defclass index-lookup (general-database-cqf) ()
  (:metaclass query-node-class))

(defclass satisfies (input-pass-through-cqf) ()
  (:metaclass query-node-class))



;; This is something which is going to deal with CLOS objects
(defclass lookup (clos-cqf-node general-database-cqf) ()
  (:metaclass query-node-class))

(defmethod class-parameter ((x lookup))
  (first (parameters x)))

(defmethod output-class ((x lookup))
  (find-class (de-quote (class-parameter x))))

(defmethod slot-parameter ((x lookup))
  (or (second (parameters x))
      (when (typep (input-type x) 'cqf-object-reference-type)
        (list 'quote
              (let ((slots (remove (class-name (object-class (input-type x)))
                                   (ccl:class-slots (output-class x))
                                   :test-not #'eq
                                   :key #'ccl:slot-definition-type)))
                (when (cdr slots)
                  (error "More than 1 slot of ~A has type ~A"
                         (class-name (output-class x))
                         (class-name (object-class (input-type x)))))
                (ccl:slot-definition-name (first slots)))))))


(defmethod expand ((x lookup))
  (make-cqf-node (class-name (class-of x))
                 (list (class-parameter x)
                       (slot-parameter x))
                 :base-vector (base-vector x)
                 :changed-blocks (changed-blocks x)
                 :input-type (input-type x)))


(defclass get-slot (clos-cqf-node general-database-cqf) ()
  (:metaclass query-node-class))

(defmethod class-parameter ((x get-slot))
  (if (second (parameters x))
      (first (parameters x))
      (when (typep (input-type x) 'cqf-object-reference-type)
        (list 'quote (class-name (object-class (input-type x)))))))

(defmethod slot-parameter ((x get-slot))
  (or (second (parameters x))
      (first (parameters x))))

(defmethod expand ((x get-slot))
  (make-cqf-node (class-name (class-of x))
                 (list (class-parameter x)
                       (slot-parameter x))
                 :input-type (input-type x)))

;; This is required because this doesn't output b and c parameters - not needed
(defmethod node-expression ((node get-slot))
  (cons (functor node)
        (parameters node)))


(defclass slot-equal (input-pass-through-cqf clos-cqf-node general-database-cqf) ()
  (:query-node-matches slot-equal slot-satisfies)
  (:metaclass query-node-class))

(defmethod slot-class ((x slot-equal))
  (if (third (parameters x))
      (find-class (de-quote (first (parameters x))))
      (if (typep (input-type x) 'cqf-object-reference-type)
          (object-class (input-type x))
          (error "Can't infer class type for ~A" x))))

(defmethod slot-parameter ((x slot-equal))
  (if (third (parameters x))
      (second (parameters x))
      (first (parameters x))))

(defmethod value-parameter ((x slot-equal))
  (first (last (parameters x))))

;; This is required because this doesn't output b and c parameters - not needed
(defmethod node-expression ((node slot-equal))
  (cons (functor node)
        (parameters node)))


;; basic database node types...
(defclass pass-through-database-cqf (input-pass-through-cqf general-database-cqf) ()
  (:documentation "CQF query node which handles column test which pass through the original id")
  (:query-node-matches column-equal column-satisfies)
  (:metaclass query-node-class))

(defclass subquery-node (general-cqf) ()
  (:metaclass query-node-class)
  (:query-node-matches product))

(defclass in/not-in (subquery-node) ()
  (:query-node-matches in not-in)
  (:metaclass query-node-class))

(defclass query (subquery-node) ()
  (:metaclass query-node-class))

;; union...

(defclass set-property (cqf-node) ()
  (:metaclass query-node-class))

;; (setf (find-class 'terminal-node) nil)
(defclass terminal-node (database-cqf-node) ()
  (:metaclass query-node-class)
  (:query-node-matches collect collect-1 collect-unique reduced))

(defclass exists (terminal-node) ()
  (:metaclass query-node-class))

;; (make-query-node-for-expression '(collect 4186))


;; Maybe I need to handle unquoted ones differently - we won't be able to do the static compilation stuff
(defun de-quote (e)
  (unless (and (listp e)
               (eq (first e) 'quote))
    (error "Not a quoted expression: ~A" e))
  (second e))

;; now, the heart of the system is this method for converting a (flat?) list of CQF nodes into a single expression




;; !!! Maybe move these, but I think I'll need them...
(defun slot-type-class (slot-definition)
  (when (symbolp (ccl:slot-definition-type slot-definition))
    (find-class (ccl:slot-definition-type slot-definition))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;; #'will-accept-input-of-type

(defmethod will-accept-input-of-type ((x t) (type t)) nil)


;; I'm not sure this is generally helpful. Should I make a subclass of cqf-value-type to mean 'naked values'?
;; (defmethod will-accept-input-of-type ((x cqf-node) (type cqf-value-type)) t)

;; exists will accept CQF value
(defmethod will-accept-input-of-type ((x exists) (type cqf-value-type)) t)

;; but in general terminals won't accept object ids - just object references
;; or scalar value types
(defmethod will-accept-input-of-type ((x terminal-node) (type cqf-value-type))
  (not (typep type 'cqf-object-id-type)))

(defmethod will-accept-input-of-type ((x slot-equal) (type cqf-object-reference-type)) t)
(defmethod will-accept-input-of-type ((x get-slot) (type cqf-object-reference-type)) t)

;; once we have determined that an object will accept input of some type we will tell it what type we will give it
;; in that way it can do its transformation...

;; now, lookup nodes will be happy with any object reference PROVIDED the class name doesn't conflict
(defmethod will-accept-input-of-type ((x lookup) (type cqf-object-reference-type))
  ;; if a class has been specified it must match the input class
  ;; if no class has been specified the input class must be the same as the class of one, and only one, of the slots
  ;; of this class.

  ;; Making all this stuff properly typed is going to be a bit of work
  t)

(defmethod will-accept-input-of-type ((x satisfies) (type cqf-object-type)) t)

;; if there are no nodes left then we will accept an object
(defmethod will-accept-input-of-type ((x null) (type cqf-object-type)) t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; As I keep doing this, let's formalize the pattern
(defun apply* (f x &rest ys)
  "Applies f (a function) to a list consisting of (car x) and (cdr x).
If there are ys then that is appended to the list to which f is applied. "
  (apply f `(,(car x) ,(cdr x)
              ,@ys)))

;; Then we can use the above to implement this...
(defun transform-for-input (previous nodes input)
  "Returns a chain of nodes by consing (if a single one) 'previous'
onto the front of the transformation of 'nodes' GIVEN THAT 'previous'
yields an object of type 'input' (which is the input to the
transformation of 'nodes') IFF 'input' is an acceptable input type for
the transformation of 'nodes'.

If it is NOT an acceptable type then this returns nil. This allows
multiple different transformations of 'previous' which yield subtly
different types for 'input' to be attempted until we find one that
works."
  (when (will-accept-input-of-type (first nodes) input)
    (when nodes
      (setf (input-type (first nodes)) input))
    (if (listp previous)
        (append previous
                (apply* #'transform nodes))
        (cons previous
              (apply* #'transform nodes)))))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; NODE-EXPRESSION

;; Just 2 straightforward methods

;; base case
(defmethod node-expression ((node cqf-node))
  (cons (functor node)
        (parameters node)))

;; database nodes
(defmethod node-expression ((node database-cqf-node))
  (append (list (functor node)
                (base-vector node)
                (changed-blocks node))
          (parameters node)))

;; ...3 straightforward methods: fear, surprise and ruthless efficency
(defmethod node-expression ((node subquery-node))
  (cons (functor node)
        (mapcar #'node-expression (parameters node))))

;; ...and an almost fanatical devotion to the Pope - 4 straightforward methods, no *amongst* the methods are...
(defmethod node-expression ((node query))
  (cons 'compose
        (mapcar #'node-expression (parameters node))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; TRANSFORM (and roll out)

;; This is the interesting bit

(defmethod transform ((x null) rest)
  (declare (ignore rest))
  nil)

;; not visible in the output
(defmethod transform ((x set-property) rest)
  (apply* #'transform rest))


(defmethod transform :before ((x terminal-node) y)
  (when y
    (error "~S must be at the end" (node-expression x))))

;; general case
(defmethod transform ((x cqf-node) rest)
  (cons x
        (apply* #'transform rest)))


;; propagate input types in this case
(defmethod transform :before ((x input-pass-through-cqf) y)
  (when y
    (setf (input-type (first y))
          (input-type x))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; NON TRIVIAL TRANSFORMATIONS

;; To know whether we can transform a CLOS node into a non-CLOS one we
;; need to know what sort of object needs to come OUT of the node





;; if we are followed anywhere by slot tests (slot-equal or slot-satisfies) then I should
;; !!! I think this doesn't work properly if the slot value is *not* an object type
;; I need to fix that...
(defmethod transform ((x lookup) rest)
  ;; first we have to ensure that we have the necessary number of parameters
  ;;  !!! what to do if we are passed an object? Unwrap it I suppose
  (if rest
      (or (transform-for-input
           (let ((index-lookup
                  (make-cqf-node 'index-lookup
                                 (list (class-table-name (output-class x))
                                       ;; !!! What if there are >1 such slots?
                                       (db-name (de-quote (or (slot-parameter x)
                                                              (error "Can't infer input type")))))
                                              
                                 :base-vector (base-vector x)
                                 :changed-blocks (changed-blocks x))))
             (cond ((typep (input-type x) 'cqf-object-type)
                    (list (make-cqf-node 'applied
                                         `(#'(lambda (x)
                                               (instance-id x ',(de-quote (slot-parameter x))))))
                          index-lookup))
                   ((typep (input-type x) 'cqf-object-id-type)
                    index-lookup)
                   (t
                    (if (cldb-foreign-key-slot-p (output-class x)
                                                 (de-quote (slot-parameter x)))
                        (list (make-cqf-node 'applied
                                             `(#'(lambda (x)
                                                   (id/instance-id x ',(de-quote (slot-parameter x))))))
                              index-lookup)
                        (list index-lookup)))))
                               
           rest
           (make-instance 'cqf-object-id-type
                          :object-class (output-class x)))
          (transform-for-input (expand x)
                               rest
                               (make-instance 'cqf-object-type
                                              :object-class (output-class x)))
          (error "~S will output an object reference but ~S will not accept this"
                 (node-expression x)
                 (node-expression (first rest))))
      ;; !!! What to do here if we have an id? Will the #'lookup function (actual) work with those? Maybe
      (list (expand x))))



;; As this is just a predicate it can always be transformed
;; if this gets passed a clos instance it would need to take its id
;; BUT I can ensure that doesn't happen...
(defmethod transform ((x slot-equal) rest)
  ;; if we are given an object id and the next node wants an object reference then we will turn our id into a reference
  ;; if we are given an object reference and the next node won't accept it, well - I don't think that will happen
  ;; now, we must check whether the next thing needs an object or not
  ;; if it does, but we are passed an id, then we need to generate object making code too...
  ;; !!! HANDLE COMPOUND SLOTS
  ;; !!! FACTOR OUT LOGIC FOR PASSING OBJECT so that I can use it in slot-satisfies too
  (cond ((typep (input-type x) 'cqf-object-id-type)
         (let ((test (make-cqf-node (cond ((eq (functor x) 'slot-equal)
                                           'column-equal)
                                          ((eq (functor x) 'slot-satisfies)
                                           'column-satisfies)
                                          (t (error "Unhandled functor: ~A" (functor x))))
                                    (list (class-table-name (slot-class x))
                                          (db-name (de-quote (slot-parameter x)))
                                          (value-parameter x))
                                    :base-vector (base-vector x)
                                    :changed-blocks (changed-blocks x))))
           (or (transform-for-input test rest (input-type x))
               (transform-for-input (list test
                                          (make-cqf-node 'map-heap-instance
                                                         (list (list 'quote
                                                                     (class-name (object-class (input-type x)))))))
                                    
                                    rest
                                    (make-instance 'cqf-object-type
                                                   :object-class
                                                   (object-class (input-type x))))
               (error "Unable to continue after slot-equal"))))
        
        ((typep (input-type x) 'cqf-object-type)
         (cons (make-cqf-node (functor x)
                              (list (slot-parameter x)
                                    (value-parameter x)))
               (apply* #'transform
                       rest)))
        (t (error "Can't handle input type ~A" (input-type x)))))


;; !!! I haven't done any optimisations for this yet.
(defmethod transform ((x get-slot) rest)
  (if (listp (de-quote (slot-parameter x)))
      ;; handling compound slot names is a straightforward rewrite...
      (apply* #'transform
              (append (let ((input-type (input-type x)))
                        (mapcar (lambda (slot)
                                  (let ((value (make-cqf-node 'get-slot
                                                              (list (list 'quote slot))
                                                              :input-type input-type)))
                                    (let ((stc (slot-type-class (find slot
                                                                      (ccl:class-slots (object-class input-type))
                                                                      :key #'ccl:slot-definition-name))))
                                      (setf input-type
                                            (if stc
                                                (make-instance 'cqf-object-type
                                                               :object-class stc)
                                                (make-instance 'cqf-value-type))))

                                    ;; propagate it through the rest of the network
                                    ;; (this will be done on every loop, but it won't matter)
                                    (setf (input-type (first rest))
                                          input-type)
                                    
                                    value))
                                (de-quote (slot-parameter x))))
                      rest))
      ;; need to do this to get pass the correct slot type
      (let ((stc (slot-type-class (find (de-quote (slot-parameter x))
                                        (ccl:class-slots (object-class (input-type x)))
                                        :key #'ccl:slot-definition-name))))
        (or (transform-for-input (expand x)
                                 rest
                                 (if (typep stc 'cldb-class)
                                     (make-instance 'cqf-object-type
                                                    :object-class stc)
                                     (make-instance 'cqf-value-type)))
            (error "Unable to transform ~A for input of type ~A" (expand x) stc)))))



;; ... I should do transformation of parameters of nested queries too
(defmethod transform ((x in/not-in) rest)
  (setf (input-type (first (parameters x)))
        (input-type x))
  (cons (make-cqf-node (functor x)
                       (apply* #'transform (parameters x)))
        (apply* #'transform rest)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; PROPAGATE-CONTEXT

(defun context-term (context name &optional error-if-not-found)
  (or (second (member name context))
      (when error-if-not-found
        (error "~A not found" name))))


;; thread context throughout the nodes
(defmethod propagate-context ((x database-cqf-node) rest context)
  (unless (context-term context :base-vector)
    (setf context
          (append (list :base-vector (gensym "BASE-VECTOR")
                        :changed-blocks (gensym "CHANGED-BLOCKS"))
                  context)))
  ;; grab it
  (setf (base-vector x) (context-term context :base-vector)
        (changed-blocks x) (context-term context :changed-blocks))
  (call-next-method x rest context))


(defmethod propagate-context ((x cqf-node) rest context)
  (if rest
      (apply* #'propagate-context rest context)
      context))

(defmethod propagate-context ((x set-property) rest context)
  (unless rest (error "Expected something after ~S" (node-expression x)))
  (apply* #'propagate-context rest (append (parameters x) context)))


(defmethod propagate-context ((x subquery-node) rest context)
  ;; I have to make the parameters into cqf nodes too in this case
  (setf (parameters x)
        (mapcar #'make-query-node-for-expression (parameters x)))
  (let ((param-context (apply* #'propagate-context (parameters x) context)))
    (append param-context
            (call-next-method x rest param-context))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; COMPOSE PAIRS OF NODES

;; This is a pretty trivial method with just 2 specialisations. Is it worth it being a method?
(defmethod compose-nodes (a (b cqf-node))
  (list 'compose a (node-expression b)))

(defmethod compose-nodes (x (terminal terminal-node))
  (append (list (functor terminal)
                (base-vector terminal)
                (changed-blocks terminal)
                x)
          (parameters terminal)))

;; #'compose-nodes



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; MACRO USING THE ABOVE

(defmacro query (&rest forms)
  ;; If any nodes require database then this will generate, on demand, syms to use for that
  (let* ((list (mapcar #'make-query-node-for-expression forms))
         (context (apply* #'propagate-context list nil))
         (transformed (apply* #'transform list))
         (composition (reduce #'compose-nodes
                              (cons (node-expression (first transformed))
                                    (cdr transformed)))))
    (if (find :base-vector context)
        `(with-database (,(context-term context :base-vector)
                          ,(context-term context :changed-blocks))
           ,composition)
        composition)))





;; I could use all the above to statically specialise compose depending on whether a and/or b need the b and c parameters
;; thus I wouldn't have to add them when not required
;; MAYBE, then, what I should do is put type declarations near the functions somewhere.
;; that way the nodes that we're generating above can look for those type declarations

;; Does CL have a way to do this? I will need Haskell style type declarations.

;; Essentially these classes which I'm creating ARE function type declarations. Maybe even more. So I don't need anything else. It allows us to do static type level polymorphism. 

