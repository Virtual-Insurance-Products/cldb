

(in-package :cldb)

;; !!! All these tests will now be broken

;; the following test works though. It's a bit awkward.
;; I need abstractions to deal with these things
(let* ((b #(0))
       (c #(nil))
       (w (writable-heap b c (length b)))
       (cons (pcons w
                    1 2)))
  (let ((b (pheap-base-vector w))
        (c (pheap-changed-blocks w)))
    (pcdr b c cons))
  )

(with-new-heap (b c #(0))
  (let* ((w (writable-heap b c 1))
         (cons (pcons w 1 2)))
    (set-heap-ref w 1 92) ; will be seen in snapshot
    (with-heap-snapshot (b c w top)
      (set-heap-ref w 1 34) ; not seen in snapshot
      ;; inside this block b and c refer to the modified heap
      ;; BUT if we change the writable heap it still won't effect the
      (unless (= (pcar b c cons) 92)
        (error "Oops - snapshot failed")))
    ))

;; clean, but very explicit:-
(with-new-writable-heap (w #(0))
  (let ((cons (pcons w 1 2)))
    (with-heap-snapshot (b c w)
      (cons (pcar b c cons)
            (pcdr b c cons)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;; I'm going to have to explicitly manage the flow of things with the database module, but I'll get to that...
;; in the meantime, here I should write more tests

;; !!! Find out how to write tests properly
(with-new-heap #(0)
  (let* ((str "Hi there - this is quite a long string")
         (pstr (pstring str)))
    (unless (= (plength pstr)
               (length str))
      (error "unqueal"))

    ;; now to pull the string out...
    (unless (equal (pobject->lisp-object pstr)
                   str)
      (error "~A and ~A don't match" str (pobject->lisp-object pstr)))
    )
  )



;; I should incorporate these into proper tests
(box-pointer (pstring "David"))

(with-new-heap #(0) (print-object (box-pointer (pcons* 1 2)) *standard-output*))
(with-new-heap #(0) (print-object (box-pointer (pcons* 1 p/nil)) *standard-output*))
(with-new-heap #(0) (print-object (box-pointer (pcons* 1 (pcons* 2 p/nil))) *standard-output*))
(with-new-heap #(0) (print-object (box-pointer (pcons* 1 (pcons* 2 3))) *standard-output*))

(with-new-heap #(0)
  (let ((a (pmake-array 100)))
    (loop for i from 0 to 99 do (setf (parray-ref a i) (* i 2)))
    (print-object (box-pointer a) *standard-output*)))


(time (with-new-heap #(0)
        (let ((a (pmake-array #x100001)))
          (assert (eql #x100001 (plength a)))
          
          )))


;; let's try sticking in 1M integers in a list of cons cells and see how quickly we can loop through it...
(with-new-heap #(0)
  (let ((l (pcons* 0 p/nil)))
    (time (loop for i from 1 to (1- 1000000)
             do (setf l (pcons* i l))))

    #+nil(let ((q 0))
      (time (loop while (not (eql l p/nil))
               do (setf l (pcdr* l))
                 (incf q)))
      q)


    (let ((q 0))
      (time (pmap-list (lambda (x)
                         (setf q x))
                       l))

      ;; this is a bit quicker, Sometimes ~10ms quicker, sometimes less
      (time (with-pobject-list-iterator (l more next)
              (loop while more do (setf q next))))

      )
    
      ))
;; 64ms to loop over 1M list items. That a lot slower than native lists
;; 35ms now that I've written pmap-list in terms of heap-ref*. Still kind of slow
;; I guess with mapping over a list we can't really take easy advantage of knowing when we cross a boundary...

(let ((l (loop for i from 1 to 1000000 collect i)))
  (time (loop for x in l sum 1))
  nil)
;; 4ms

;; I wonder how to get this faster




;; Let's have a look at mapping over an array...

(with-new-heap #(0)
  (let* ((size #x100001)
         (end (1- size))
         (a (pmake-array size))
         (q nil))

    #+nil(time (dotimes (i size)
            (setf (parray-ref a i) i)))

    (time (dotimes (i size)
            (setf q (parray-ref a i)))) ; 74ms

    (let ((base (pheap-base-vector *heap*))
          (changed (pheap-changed-blocks *heap*)))
      (time (dotimes (i size)
              (setf q (parray-ref-32 a i base changed)))))
    
    ))

;; I could make various interesting data structures in order to tweak the efficiency of mapping over all ids for a value



;; let's test string comparison.
;; This is supposed to be fairly efficient - take a lisp string (not on the heap) and test it against a pstring...

(comparator "David")
(comparator "David is my name")


(with-new-heap #(0)
  (assert (= (comparator "David") ; comparator yields a fixnum
             (pstring "David")))

  (assert (not (= (comparator "DaviD")
                  (pstring "David"))))

  (assert (funcall (comparator "This is a longer string")
                   (pstring "This is a longer string")))

  (assert (not (funcall (comparator "This is a longer strinG")
                        (pstring "This is a longer string"))))

  (assert (not (funcall (comparator "This is a longer string with some")
                        (pstring "This is a longer string"))))

  (assert (not (funcall (comparator "This is a longer string")
                        (pstring "This is a longer string with some"))))

  *heap*)

;; If I wanted to intern all the strings to save space then I could do so with a 'hash table' thingy which was just a list of strings.
;; Finding existing strings should be easy enough. The strings will just get inserted randomly in memory, but will be recycled each time.
;; In fact, I guess I could then use a quicker comparator, because I would find the string once, yielding a pointer. Any reference to that string would then, necessarily, be the same pointer.
;; Probably a Good Idea.

;; I can *even* add it in later with minimal cost if I make the comparator find an interned string and check for that first, but check for a non interned one later.


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; so I can isolate changes like this:-
(let ((d (make-database)))
  (block foo
    (with-database-transaction (w d)
      (setf (parray-ref w +database-header+ 0) 9)
      ;; doing this stops the transaction from comitting:-
      ;; (return-from foo :nope)
      ))
  (with-database (b c nil d)
                 (parray-ref b c +database-header+ 0)))

(let ((d (make-database)))
  (with-database (b c nil d)
                 (list (find-unique-string b c "David")
                       (find-unique-string b c "this is a longer string"))))

(let ((q (make-database))
      (l nil))
  (with-database-transaction (w q)
    (setf l (add-unique-string w "this is a longer string"))
    (let ((b (pheap-base-vector w))
          (c (pheap-changed-blocks w)))
      (print (box-pointer b c l) *standard-output*)) )
  (with-database (b c
                    nil q)
                 ;; (pcdr b c l)
                 (print (box-pointer b c l) *standard-output*))
  )

(box-pointer nil nil
             (with-database-transaction (w (make-database))
               ))

(let ((d (make-database)))
  (with-database-transaction (w d)
    (add-unique-string w "this is a longer string"))
  #+nil(with-database (b c d)
                 (box-pointer b c
                              (hash-table-list b c (string-intern-table b c)
                                               "this is a longer string")))
  (with-database (b c nil d)
                 (print (box-pointer b c (find-unique-string b c "this is a longer string"))
                        *standard-output*)
                 (list (find-unique-string b c "this is a longer string")
                       (find-unique-string b c "this is another long string"))))

(let ((d (make-database)))
  (with-database-transaction (w d)
    (intern-string w "This is a somewhat long string")
    d))

(let ((*current-database* (make-database)))
  (with-database-transaction (w)
    (intern-string w "This is a long string which I am interning"))
  (snapshot-database "/Users/david/Desktop/new-database"))

(close-database)
(time (open-database "/Users/david/Desktop/new-database"))

(with-database (b c)
               (find-unique-string b c "This is a long string which I am interning")
               )

(make-database)

;; !!! Try and make changed blocks with holes - demand create the block


(let ((d (make-database))
      (col nil))
  (with-database-transaction (w d)
    (setf col (make-simple-column w 200 "foo" "bar"))

    ;; now let's try setting a column value...
    (setf (column-value w col 1) 256
          (column-value w col 2) "This is a nice string"
          (column-value w col 3) 120056/100)

    )
  
  (with-database (b c nil d)
                 (list (pobject->lisp-object b c (column-slot b c col column.table))
                       (column-value b c col 1)
                       (column-value b c col 2)
                       (column-value b c col 3)

                       ;; there is something there. How to inspect?
                       (format nil "~S" (box-pointer b c (hash-table-list b c
                                                                          (column-slot b c col column.index)
                                                                          256)))
                       )
                 
                 ))



(let ((d (make-database))
      (col nil)
      (length #x100001))
  (with-database-transaction (w d)
    (setf col (make-simple-column w length "foo" "bar"))

    ;; now let's try setting a column value...
    ;; 150ms to set 1M rows. Not bad, though somewhat slower than I would like.
    ;; (that's with  the setf)
    ;; I think I can do better...
    ;; 75ms if we use the lower level set-column-value
    ;; that will drop when we add in indexes though.
    ;; I wonder if I can get this any quicker
    ;; now down to 62--66ms by doing set-heap-ref directly instead of going via array
    ;; That should be ok, but I need to check the array limit - added as an explicit param
    ;; 0.575s with index. Not too bad
    (let* ((b (pheap-base-vector w))
           (c (pheap-changed-blocks w))
           (id->value (column-slot (pheap-base-vector w)
                                   (pheap-changed-blocks w)
                                   col column.id->value))
           (index (column-slot (pheap-base-vector w)
                               (pheap-changed-blocks w)
                               col column.index)))
      (time (dotimes (i length)
              ;; (setf (column-value w col i) (* 2 i))
              (set-column-value w b c
                                id->value
                                nil
                                index
                                ;; nil
                                i (* 2 i)
                                length)

              )))

    )
  
  (with-database (b c nil d)
                 (list (pobject->lisp-object b c (column-slot b c col column.table))
                       (column-value b c col 1)
                       (column-value b c col 2)
                       (column-value b c col 3)
                       (ids-for-value b c col 256)
                       (ids-for-value b c col 123)
                       )
                 
                 ))

;; the following now works, but does not guarantee that each id only occurs once
(let ((d (make-database))
      (col nil))
  (with-database-transaction (w d)
    (setf col (make-simple-column w 100 "foo" "bar"))
    (make-simple-column w 1000 "foo" "blah" :index-p nil)
    (dotimes (i 20)
      (setf (column-value w col i) 45))

    ;; (setf (column-value w col 10) 5)
    (setf (column-value w col 11) 45)
    (setf (column-value w col 11) 45)
    (setf (column-value w col 11) 45)

    #+nil(dotimes (i 20)
      (setf (column-value w col 50) 123))
    
    )
  
  (with-database (b c nil d)
                 (let ((col (find-column b c "foo" "bar")))
                   (list (let ((r nil))
                           (map-ids-for-value b c col (lambda (id)
                                                        (push id r))
                                              45)
                           r)
                         col
                         (name col)
                         (table col)
                         (find-column b c "foo" "blah")

                         
                         
                         )))

  ;; this only returns 1, as a list. BUT this might be not that efficient. 
  #+nil(with-database (b c nil d)
                 (ids-for-value b c col 123)))


(let ((d (make-database)))
  (with-database-transaction (w d)
    (list (= (intern-string w "This is a longish string")
             (intern-string w "This is a longish string"))
          (pobject->lisp-object (pheap-base-vector w)
                                (pheap-changed-blocks w)
                                (intern-string w "This is a longish string")))))




(in-package :cldb)

(with-database (b c)
               (awhen (find-column b c "brand" "name")
                 (list (column-value b c it 3)
                       (ids-for-value b c it "Jackson Lee Underwriting")
                       (ids-for-value b c it "blah"))))

(with-database (b c)
               (awhen (find-column b c "quote" "broker_product")
                 (time (ids-for-value b c it 30716))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Metaclass demonstration

(defclass broker ()
  ((name :reader name)
   ;; store the computed quotes getting query
   (quotes :allocation :class))
  (:metaclass cldb-class))

;; (setf (find-class 'broker) nil)
;; (slot-value (get-instance 'broker 4186) 'quotes)

;; then we can do this sort of thing
(defmethod broker-quotes ((self broker))
  (with-slots (base-vector changed-blocks broker)
      self
    (collect-1 base-vector changed-blocks
               (or (slot-value self 'quotes)
                   (setf (slot-value self 'quotes)
                         <broker-quotes-query>))
               broker)))

;; deferring the initialization of the broker quotes query until first use might be good
;; we can't initialize it at build time because we won't have loaded the database
;; BUT by the time we try to use it we should be good.


(defclass broker-product ()
  ((broker :type broker)
   (vip-commission))
  (:metaclass cldb-class))


(defclass cldb-quote ()
  ((broker-product :type broker-product))
  (:metaclass cldb-class)
  (:table-name quote))
;; (get-instance 'cldb-quote 12345)

(defclass qstate ()
  ((description :reader description))
  (:metaclass cldb-class))


(defclass quote-state ()
  ((quote :type cldb-quote)
   (qstate :type qstate :reader qstate)
   (is-current :reader is-current))
  (:metaclass cldb-class))
;; (find-class 'quote-state)

(defclass underwriter ()
  (name currency)
  (:metaclass cldb-class))

;; (get-instance 'underwriter 98)

;; (setf (find-class 'underwriter-product) nil)
(defclass underwriter-product ()
  ((underwriter :type underwriter)
   name
   comment
   payment-type
   category
   sequence
   sage-code
   bank-code
   )
  (:metaclass cldb-class))

(get-instance 'underwriter-product 6399)

(with-database (b c)
  (heap-instance b c 'broker-product :broker-product 1845986))

(with-database (b c)
  (heap-instance b c 'quote-state :quote-state 1045981))


;; it will be interesting to see how quickly we get a quote's current state. It should be very quick though, even if we go via
;; the metaclass after getting the current states for the quote...


(with-database (b c)
               (time (length (find-instances b c 'broker-product :broker 4186))))
;; 12ms. That's relatively slow. How many? 6053

(time (description (qstate (find t
                                 (with-database (b c)
                                                (find-instances b c 'quote-state :quote 585028))
                                 :key #'is-current))))
;; ~74us after warmup


;; query composition...
(time
 (collect (with-database (b c)
                         (compose (index-lookup b c "broker_product" "broker")
                                  ;; this selects only ones with is_current being t
                                  (column-equal b c "broker_product" "is_current" t)
                                  (column-satisfies b c "broker_product" "vip_commission"
                                                    (lambda (v)
                                                      (> v 100)))
                                  (index-lookup b c "quote" "broker_product")
                                  ;; (index-lookup b c "issued_policy" "quote")
                                  (map-heap-instance b c 'cldb-quote)
                                  ))
   4186))

;; the above composable function thingies will be a good basis for building a general query engine.
;; We will look at the tables in turn, hopefully start with a map-ids and then put in any column equal things

;; now, the only thing is: the constant b and c are noise
;; it would be good if I could just say...
(collect (with-database (b c)
                        (query b c
                               (index-lookup "broker_product" "broker")
                               (column-equal "broker_product" "is_current" t)
                               (column-satisfies "broker_product" "vip_commission"
                                                 (lambda (v)
                                                   (> v 100)))
                               (index-lookup "quote" "broker_product")
                               (map-heap-instance 'cldb-quote)
                               ))
  4186)

;; OTOH, maybe it makes more sense to have the query accept b and c
;; but w/o a b and c in the first place it can't find the column, and that's super useful to get things out as quick as possible
;; I think I'll leave it for the mo

;; how can I use the above to find closed quotes for some broker?
;; do I need to somehow nest things?


(defun quotes-in-state (b c state)
  (in (query b c
             (index-lookup "quote_state" "quote")
             (column-equal 'quote-state :is_current t)
             (get-column-value 'quote-state :qstate)
             (column-equal "qstate" "description" state))))

(let ((query (with-database (b c)
               (query b c
                      (index-lookup 'broker-product :broker)
                      (index-lookup 'quote :broker-product)
                      ;; the nice thing is the above can be pulled out into a separate function to get quotes in some state
                      (quotes-in-state "closed")))))
  (time (collect query
          4186)))

(time (collect-1 (with-database (b c)
                   (query b c
                          (index-lookup "broker_product" "broker")
                          (index-lookup "quote" "broker_product")
                          (in (query b c
                                     (index-lookup "quote_state" "quote")
                                     (column-equal "quote_state" "is_current" t)
                                     (get-column-value "quote_state" "qstate")
                                     (column-equal "qstate" "description" "closed")))))
        4186))

(time (let ((q nil))
        (funcall (funcall (with-database (b c)
                            (query b c
                                   (index-lookup "quote_state" "quote")
                                   (column-equal "quote_state" "is_current" t)
                                   (get-column-value "quote_state" "qstate")
                                   (column-equal "qstate" "description" "closed")))
                          398881)
                 (lambda (x)
                   (setf q x)))))

(time (collect-1 (with-database (b c)
                   (query b c
                          (index-lookup "quote_state" "quote")
                          (column-equal "quote_state" "is_current" t)
                          (get-column-value "quote_state" "qstate")
                          (get-column-value "qstate" "description")))
                 12345))


;; It's interesting that I *have* to start from broker-product because I don't have a way to map over all quotes at the moment.
;; I could kind of use the index to do so. 


;; ~50us to check for access rights
(defun user-has-access-right (user right)
  (collect-1 (with-database (b c)
               (query b c
                      (index-lookup "access_right" "user_account")
                      (column-equal "access_right" "right"
                                    right)))
             
             user))


(in-package :vip)

;; This isn't going to work once I make the switch...
(database-transaction
  (loop for (quote) in (dquery "select id from quote where id not in (select quote from re_issued_policy)")
     for pf = (s "~A~A" (abel "policyFile-")
                 quote)
       for query = (list pf (abel "policyQuote") :x)
     unless (equal (sort (quick-?-lookup query) #'string-lessp)
                   (sort (quick-?-lookup-2 query) #'string-lessp))
       do (error "Discrepancy for ~A" quote)
       ))
