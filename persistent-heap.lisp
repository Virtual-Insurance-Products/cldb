

(in-package :cldb)


;; A heap consists of a base vector and an array of changed blocks
;; if we want to start writing into a heap we also need to track which blocks need copying and which we already own
;; this is done using the following structure:-

;; this structure encapsulates what is required for a writable heap
(defstruct pheap
  (base-vector #(0))
  (top 1)
  ;; I'm starting off with a small number of these. They will be allocated on demand
  (changed-blocks (make-array 10 :initial-element nil :adjustable t))
  (owned-blocks (make-array 10 :adjustable t :initial-element nil)))

;; but if we just want to read we only need 2 things: the base vector and the changed blocks.

;; Now, if we have those 2 things for reading and we wish to start writing then we do this:-
(defun writable-heap (base-vector changed-blocks size)
  (make-pheap :base-vector base-vector
              :top size
              :changed-blocks (let ((a (make-array (length changed-blocks)
                                                   :adjustable t :initial-element nil)))
                                (replace a changed-blocks)
                                a)
              :owned-blocks (make-array (length changed-blocks) :adjustable t :initial-element nil)))

;; (which also needs the heap size passing in, which we need to track separately)

(defmacro with-new-heap ((base changed vec) &body forms)
  `(let* ((,base ,vec)
          (,changed (make-array (length ,base) :initial-element nil)))
     ,@forms))

;; we need not really clone the changed blocks array IFF we are definitely not going to write to it again
(defmacro with-heap-snapshot ((base changed writable-heap &optional top)
                              &body forms)
  (let ((w (gensym "writable")))
    `(let* ((,w ,writable-heap)
            (,base (pheap-base-vector ,w))
            (,changed (copy-seq (pheap-changed-blocks ,w)))
            ,@(when top
                `((,top (pheap-top ,w)))))
       ;; by clearing out the changed blocks we will ensure that the snapshot doesn't get mutated outside by accident.
       ;; I could just somehow 'seal' the writable heap instead (by zeroing out this), since it shouldn't continue to be written
       ;; I suppose. Should it? I don't know.
       (setf (pheap-owned-blocks ,w)
             (make-array (length ,changed) :initial-element nil :adjustable t))
       ,@forms)))

(defmacro with-new-writable-heap ((w vec) &body forms)
  (let ((b (gensym "base"))
        (c (gensym "changed"))
        (v (gensym "vec")))
    `(let ((,v ,vec))
       (with-new-heap (,b ,c ,vec)
         (let ((,w (writable-heap ,b ,c (length ,v))))
           ,@forms)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; HEAP DATA FORMAT

;; The data format of things on the heap, and pointers to them, is as follows...


(defmacro deftag (name bits)
  `(defconstant ,name (ash ,bits 56)))

(deftag +tag-bits+ #b1111)
(defconstant +address-bits+ (1- (ash 1 40)))
(defconstant +length-bits+ (ash #xFFFF 40))
(defconstant +fixnum-bits+ (logior +address-bits+ +length-bits+))


(deftag p/fixnum #b0000)
(deftag p/negative-fixnum #b0001)
(deftag p/small-rational #b0010)
(deftag p/negative-small-rational #b0011)
(deftag p/const #b0100) ; various constants: nil, t, all the characters, empty string, empty array etc

;; given the p/const tag we can now define t, nil, all the characters and so on as follows:-
;; (defconstant p/character (logior p/const (ash 1 55)))
(defconstant p/nil p/const)
(defconstant p/t (logior p/const (ash 1 40)))
(defconstant p/null 0) ; zeros mean null

;; for characters they are tagged as constant #2 and the address bits will be used as character number...
(defconstant p/character (logior p/const (ash 2 40)))

(defconstant p/empty-string (logior p/const (ash 3 40)))
(defconstant p/empty-array-t (logior p/const (ash 4 40)))

(defconstant p/zero (logior p/const (ash 5 40)))


;; then, of course, we want cons cells...
(deftag p/cons #b0101)

;; now we need some handy vector types...
(deftag p/string-16 #b0110) ; string with up to 2**16 characters (NOT 2**16-1, since the empty string is a special constant)
(deftag p/array-t-16 #b0111) ; array with up to 2**16 elements (as above)

(deftag p/array-t-32 #b1000) ; an array of up to 2**32 elements BUT it's length must be a multiple of 2**16+1 since we encode the index of the last element in the pointer's 16 length bits and just ash it by 16. Encoding the index of the last element gives the +1

(deftag p/other #b1110) ; look in the 'length' field for extended tagged objects...
(deftag p/lisp-object #b1111) ; lisp object serialize with the reader - rather expensive
;; (defconstant p/lisp-object #b1111) ; any other lisp object which must be read with the reader (expensive)


(declaim (inline address-field))
(defun address-field (pobject)
  (declare (type fixnum pobject))
  (declare (optimize (speed 3)))
  (the fixnum (logand pobject +address-bits+)))

(declaim (inline length-field))
(defun length-field (pobject)
  (declare (type fixnum pobject))
  (declare (optimize (speed 3)))
  (the fixnum
       (ash
        (the fixnum
             (logand pobject +length-bits+))
        -40)))

(defun address-field-bytes (pobject &optional (count 5))
  (let ((v (make-array count :element-type '(unsigned-byte 8))))
    (loop for i from 0 to (1- count)
       do (setf (aref v i)
                (ldb (byte 8 (* 8 i)) pobject)))
    v))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; HEAP READING


(declaim (inline heap-ref))

;; Read an object from a heap made of a base vector and changed blocks
;; each changed block contains #x1000 entries (4096)
(defun heap-ref (base changed ref)
  (declare (optimize (speed 3)))
  (declare (type fixnum ref))
  ;; (format *standard-output* "Heap ref ~A...~%" ref)
  (let ((bucket (aref changed
                      (ash ref -12))))
    (if bucket
        (aref bucket (logand ref #xFFF))
        (aref base ref))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; HEAP WRITING (with COW)

(defvar +empty-block+ (make-array 4096 :element-type '(unsigned-byte 64)))

(defun can-write-block (heap i
                        ;; Experimental file-hole changes
                        ;; &optional (must-exist t)
                             )
  (let ((changed-blocks (pheap-changed-blocks heap)))
    (and (< i (length changed-blocks))
         (aref (pheap-owned-blocks heap) i)
         #+nil(or (not must-exist)
                  (aref changed-blocks i))
         (aref changed-blocks i))))

(defun make-block (index previous-block base-vector)
  (if (and (not previous-block)
           (not (> (length base-vector)
                   (* 4096 index))))
      +empty-block+
      (let ((a (make-array 4096 :element-type '(unsigned-byte 64))))
        (if previous-block
            (unless (eql previous-block +empty-block+)
              (replace a previous-block))
            (when (> (length base-vector)
                     (* 4096 index))
              (replace a base-vector :start2 (* 4096 index))))
        a)))

(defun create-block (heap i)
  (unless (< i (length (pheap-changed-blocks heap)))
    (adjust-array (pheap-changed-blocks heap)
                  (* i 2) :initial-element nil)
    (adjust-array (pheap-owned-blocks heap)
                  (* i 2) :initial-element nil))
  
  
  ;; now we can safely create the new block...
  (let ((new-block (make-block i
                               (aref (pheap-changed-blocks heap) i)
                               (pheap-base-vector heap))))
    (setf (aref (pheap-owned-blocks heap) i)
          (not (eql new-block +empty-block+)) ; we don't own the empty block

          (aref (pheap-changed-blocks heap) i)
          new-block)))

(defun extend-heap (heap amount)
  (let ((start (pheap-top heap)))
    ;; make sure there are writable blocks here...
    (loop for i from (ash start -12) to (ash (+ start amount) -12)
       unless (can-write-block heap i ; nil ; file hole change
                               )
       do (create-block heap i))
    
    (incf (pheap-top heap) amount)))


(defun add-to-heap (heap word)
  ;; extend-heap necessarily causes the heap to be made writable at the requested location
  (let ((addr (1- (extend-heap heap 1))))
    (setf (aref (aref (pheap-changed-blocks heap)
                      (ash addr -12))
                (logand addr 4095))
          word)
    addr))

(defun set-heap-ref (heap index value)
  (when (> index (pheap-top heap))
    (error "Above top of heap!"))
  ;; (format t "Set heap ref ~A = ~A~%" index value)
  (let ((bucket (ash index -12)))
    (setf (aref (or (can-write-block heap bucket)
                    (create-block heap bucket))
                (logand index #xFFF))
          value)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; SAVING AND LOADING A HEAP

(defstruct file-heap
  base-vector changed-blocks
  top mapped-vector)


;; This operates on a writable heap - why save it if you haven't written it?
;; (should it just operate on a file heap?)
(defun save-heap (heap file &key (replace :error))
  (declare (optimize (speed 3)))
  (with-open-file (stream (if (eq replace :atomic)
                              (concatenate 'string file "-temp")
                              file)
                          :direction :output
                          :if-does-not-exist :create
                          :if-exists (if (eq replace :atomic)
                                         :supersede
                                         replace)
                          :element-type '(unsigned-byte 64))
    (let* ((base (pheap-base-vector heap))
           (changed-blocks (pheap-changed-blocks heap))
           (top (pheap-top heap))
           (last-index (ash top -12)))
      (loop for index from 0 to last-index
         for block-start = (* 4096 index)
         for block-next = (+ 4096 block-start)
         for changed = (aref changed-blocks index)
           when (= 0 (mod index 10000)) do (ccl:gc)
         do
         ;; (format t "Write block from ~A to ~A~%" block-start block-next)
           (if (and (> index 0)
                    (< index last-index)
                    (or (eql changed +empty-block+)
                        (and (not changed)
                             (not (loop for i from block-start
                                     to (1- block-next)
                                     when (not (= 0 (aref base i)))
                                     return t)))))
               ;; seek instead of writing null bytes
               ;; https://en.wikipedia.org/wiki/Sparse_file
               (progn
                 (file-position stream (+ (file-position stream)
                                          4096))
                 (when (not changed)
                   ;; make a note that it's empty so I can snapshot quicker next time
                   (setf (aref changed-blocks index) +empty-block+)))
               (write-sequence (or changed base)
                               stream
                               :start (if (= index 0)
                                          1 ; !!! don't need to write the first word (which will always be null)
                                          (if changed
                                              0 block-start))
                               :end (if (> block-next top)
                                        (if changed
                                            (logand top 4095)
                                            top)
                                        (if changed
                                            nil
                                            block-next)))))))
  (when (eq replace :atomic)
    (ccl::unix-rename (concatenate 'string file "-temp") file)))


;; when the heap file is opened this will append an extra word at the start
;; this word must be ignored when writing the heap to disk...
;; I think I should make the changed block list the right length 

;; You can also open a heap, which returns the base array, changed block list (initially empty) and the mapped file
(defun open-heap (file)
  (let ((m (ccl:map-file-to-ivector file '(unsigned-byte 64))))
    (multiple-value-bind (arr offset)
        (array-displacement m)
      (declare (ignore offset))         ; which must be 1
      (let* ((blocks (make-array (1+ (ash (length arr) -12))
                                 :initial-element nil))
             (heap (make-file-heap :base-vector arr
                                   :changed-blocks blocks
                                   :top (length arr)
                                   :mapped-vector m)))
        ;; The ivector may be shared between heap objects (see the macro
        ;; `with-database-transaction' so we register the finalizer for
        ;; the ivector, not for the heap.
        (ccl:terminate-when-unreachable m #'ccl:unmap-ivector)
        heap))))

;; at the moment this works on a mapped vector as returned as the 3rd list item of open-heap
(defun close-mapped-file (file-heap)
  ;; The vector may be shared between different file heaps so closing
  ;; the mapped file just removed the reference. Unmapping the vector is
  ;; left to the finalizer.
  (setf (file-heap-mapped-vector file-heap) nil))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; CREATING OBJECTS ON THE HEAP

;; Now we can start to create objects on a writable heap...

;; !!! Should this say 'the fixnum'? Probably
(defmacro make-pointer (tag address &optional length-bits)
  (if length-bits
      `(the fixnum (logior (the fixnum ,tag)
                           (the fixnum (ash ,length-bits 40))
                           (the fixnum ,address)))
      `(the fixnum (logior (the fixnum ,tag)
                           (the fixnum ,address)))))


(defun pcons (heap a b)
  (let ((addr (add-to-heap heap a)))
    (add-to-heap heap b)
    (make-pointer p/cons addr)))


(defun pmake-array (heap length &key (element-type t))
  (declare (ignore element-type))
  (cond ((= length 0)
         p/empty-array-t)
        ((<= length (ash 1 16))
         (let ((addr (pheap-top heap)))
           (extend-heap heap length)
           (make-pointer p/array-t-16
                         addr
                         ;; have to subtract 1 as the 'length' field actually encodes the index of the last element
                         (1- length))))
        
        ((and (> length (ash 1 16))
              (= (logand length #xFFFF) 1)
              (<= length (ash 1 32)))
         (let ((addr (pheap-top heap)))
           (extend-heap heap length)
           (make-pointer p/array-t-32
                         addr
                         ;; Shift right by 16 so what we encode will
                         ;; be the index of the last element divided
                         ;; by #xFFFF (1- (ash length (- 40 16)))
                         (ash (1- length) -16))))
        
        (t (error "Can't make array of ~A elements yet" length))))





;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; ACCESSING DATA FROM OBJECTS ON THE HEAP

;; 1. lists (cons cells)

(declaim (inline pcar))
(defun pcar (base changed cons)
  (if (= cons p/nil)
      p/nil
      (heap-ref base changed (address-field cons))))

(declaim (inline pcdr))
(defun pcdr (base changed cons)
  (if (= cons p/nil)
      p/nil
      (heap-ref base changed (1+ (address-field cons)))))


(defmacro with-pobject-list-iterator ((base changed plist &optional (more 'more) (next 'next)
                                            (current (gensym "current")))
                                      &body body)
  (let ((object (gensym "object")))
    `(let ((,current ,plist)
           (,object nil))
       (symbol-macrolet ((,more (not (= ,current p/nil)))
                         (,next (progn (setf ,object (pcar ,base ,changed ,current)
                                             ,current (pcdr ,base ,changed ,current))
                                       ,object)))
         ,@body))))

(defun pmap-list (base changed f list)
  (with-pobject-list-iterator (base changed list more next)
    (loop while more do (funcall f next))))

;; 2. arrays...

(declaim (inline parray-16-length))
(defun parray-16-length (pobject)
  (declare (type fixnum pobject))
  (declare (optimize (speed 3)))
  (the fixnum (1+ (length-field pobject))))

(defun parray-32-length (pobject)
  (declare (type fixnum pobject))
  (declare (optimize (speed 3)))
  (the fixnum (1+ (the fixnum
                       ;; (ash (length-field pobject) 16) ; is the compiler smart enough to fold the 2 ash oeprations?
                       (ash
                        (the fixnum
                             (logand pobject +length-bits+))
                        ;; only shift it 24 bits - basically shift -40 then shift 16 to get multiple it by 2**16
                        -24)))))

;; now let's ask for the length of some pointers to things...
(declaim (inline plength))
(defun plength (base changed pobject)
  (declare (type fixnum pobject))
  (declare (optimize (speed 3)))
  (declare (ignore base changed)) ; but they will be needed later on...
  (let ((tag (logand pobject +tag-bits+)))
    (declare (type fixnum tag))
    (cond ((or (eql pobject p/empty-array-t)
               (eql pobject p/empty-string))
           0)
          ((or (eql tag p/string-16)
               (eql tag p/array-t-16))
           (the fixnum
                (parray-16-length pobject)))

          ((eql tag p/array-t-32)
           (the fixnum
                (parray-32-length pobject)))

          ;; for bigger arrays we will have to deref the array and look for its length field, which will be the object at
          ;; its address
          
          )
    ))
;; (disassemble 'plength)

;; (plength (pmake-array 0))
;; (plength (pmake-array 123))
;; (plength (pmake-array 65536))
;; (plength (pmake-array 100000))

;; This doesn't do bounds checking
;; (defmacro parray-ref* (pobject index))

(defmacro parray-ref* (base changed pobject index)
  `(heap-ref ,base ,changed (the fixnum (+ ,index (address-field ,pobject)))))

(defun parray-ref (base changed pobject index)
  (declare (type fixnum pobject index))
  (unless (< index (plength base changed pobject))
    (error "Out of bounds!"))
  (parray-ref* base changed pobject index))
;; (disassemble 'parray-ref)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; WRITING DATA TO HEAP OBJECTS...

;; It would be good to test and optimize speed of these things
(declaim (inline set-array-ref*))
(defun set-array-ref* (heap pobject index value)
  (declare (optimize (speed 3))
           (type fixnum pobject index value))
  (set-heap-ref heap (the fixnum (+ index
                                    (the fixnum (address-field pobject))))
                value))

(defun (setf parray-ref) (value heap pobject index)
  (declare (type fixnum pobject index value))
  ;; !!! I might optimize this for 16/32 arrays
  (unless (< index (plength (pheap-base-vector heap)
                            (pheap-changed-blocks heap)
                            pobject))
    (error "Out of bounds!"))
  ;; !!! This wouldn't work for arrays whose length was stored at the address, so I need to account for that if/when I make those
  (set-array-ref* heap pobject index value))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; STRINGS

;; (but see string interning later on)

(defun bytes-to-word (byte-vector start)
  (if (>= (length byte-vector) (+ start 8))
      (the (unsigned-byte 64)
           (macrolet ((x ()
                        `(logior ,@ (loop for i from 0 to 7
                                       collect (if (= i 0)
                                                   `(aref byte-vector start)
                                                   `(ash (aref byte-vector
                                                               (+ start ,i))
                                                         ,(* 8 i)))))))
             (x)))
      (loop for i from start to (1- (length byte-vector))
         for shift from 0
         sum (ash (aref byte-vector i) (* 8 shift)))))

(defun insert-byte-vector (heap byte-vector)
  (let* ((length (length byte-vector))
         (word-length (+ (ash length -3)
                         (if (eql 0 (logand length #b111))
                             0 1)))
         (address (pheap-top heap)))
    (extend-heap heap word-length)
    (let ((i 0))
      (loop for d from address
         while (< i length)
         do
           (set-heap-ref heap d (bytes-to-word byte-vector i))
           (incf i 8)))
    address))


;; again - this should be optimizable
(defun read-byte-vector (base changed byte-vector address)
  (loop for i from 0 to (1- (length byte-vector))
     for addr = (+ address (ash i -3))
     for shift = (logand i #b111)
     do
       (setf (aref byte-vector i)
             (logand (ash (heap-ref base changed addr)
                          (- (* 8 shift)))
                     #xFF))))


;; make and return a pstring from a normal lisp string
;; (pass in the byte vector for the string if you already made it)
(defun pstring (heap string &optional (vec (unless (equal string "")
                                             (trivial-utf-8:string-to-utf-8-bytes string))))
  (if (equal string "")
      p/empty-string
      (if (< (length vec) 6)
          ;; it will fit in the pointer!
          (make-pointer p/string-16
                        (loop for x across vec
                           for i from 0
                           sum (ash x (* 8 i)))
                        (1- (length vec)))
          (if (<= (length vec) 65536)
              (make-pointer p/string-16
                            (insert-byte-vector heap vec)
                            (1- (length vec)))
              (error "Don't know how to write long strings yet (~A bytes)" (length vec))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; GENERALIZED OBJECT RETRIEVAL

;; (but this isn't going to be generally useful)

;; now, we need to turn various lisp things into pobject things and vice versa
;; Let's start by interpreting some pobject things
(defun pobject->lisp-object (base changed pobject)
  (assert (typep pobject 'fixnum))
  (cond ((= pobject p/nil) nil)
        ((= pobject p/t) t)
        ((= pobject p/null) :null)
        ((= pobject p/zero) 0)
        ((= pobject p/empty-string) "")
        ((= pobject p/empty-array-t) (make-array 0 :element-type t))
        ((= p/character
            (logand pobject
                    (ash #xFFFFF 40)))
         (code-char (address-field pobject)))
        (t (let ((tag (logand pobject +tag-bits+)))
             (cond ((= tag p/fixnum) pobject)
                   ((= tag p/negative-fixnum) (- (logand pobject
                                                         +fixnum-bits+)))
                   ((= tag p/small-rational)
                    (/ (address-field pobject)
                       (length-field pobject)))
                   ((= tag p/negative-small-rational)
                    (- (/ (address-field pobject)
                          (length-field pobject))))
                   ((= tag p/string-16)
                    (let ((length (parray-16-length pobject)))
                      (if (< length 6)
                          (trivial-utf-8:utf-8-bytes-to-string (address-field-bytes pobject length))
                          (let ((vec (make-array length :element-type '(unsigned-byte 8))))
                            (read-byte-vector base changed vec (address-field pobject))
                            (trivial-utf-8:utf-8-bytes-to-string vec)))))
                   (t (error "Can't understand ~A as a pobject" pobject)))))))

;; The following might not be that useful
(defun lisp-object->pobject (x)
  (cond ((not x)
         p/nil)
        ((eq x 0) p/zero)
        ((eq x :null) p/null)
        ((eq x t) p/t)
        ((and (integerp x)
              (>= x 0)
              (< x #.(ash 1 56)))
         x)
        ((and (integerp x)
              (< x 0)
              (> x (- #. (ash 1 56))))
         (logior p/negative-fixnum (- x)))
        ((and (rationalp x)
              (> x 0)
              (<= (numerator x) +address-bits+)
              (< (denominator x) #x10000))
         (logior p/small-rational
                 (numerator x)
                 (ash (denominator x) 40)))
        ((and (rationalp x)
              (< x 0)
              (<= (- (numerator x)) +address-bits+)
              (< (denominator x) #x10000))
         (logior p/negative-small-rational
                 (- (numerator x))
                 (ash (denominator x) 40)))
        
        ((characterp x)
         (logior p/character
                 (char-code x)))

        (t (error "Can't convert ~A to an immediate pobject" x))))

;; (lisp-object->pobject 123)
;; (lisp-object->pobject -123)
;; (lisp-object->pobject 23/100)
;; (lisp-object->pobject -23/100)
;; (lisp-object->pobject #\Null) 
;; (lisp-object->pobject #\a) 
;; (lisp-object->pobject t)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; BOXING HEAP OBJECTS...


;; Most things will work in terms of raw pointers, but when, for example, we wish to pprint things we can just box them...

(defclass boxed-pheap-object ()
  ((pointer :initarg :pointer :reader pointer :type fixnum)
   (tag :initform nil :reader tag)
   (base-vector :initarg :base-vector :reader base-vector)
   (changed-blocks :initarg :changed-blocks :reader changed-blocks)))

(macrolet ((make-classes (&rest list)
             `(progn
                ,@(loop for x in list
                     collect `(defclass ,x (boxed-pheap-object)
                                ((tag :initform ,x)))

                     collect `(defmethod %box-pointer (base changed (pointer fixnum) (tag (eql ,x)))
                                (make-instance ',x :pointer pointer
                                               :base-vector base
                                               :changed-blocks changed))
                       ))))
  (make-classes p/fixnum p/negative-fixnum p/small-rational p/negative-small-rational
                p/const
                p/cons p/string-16 p/array-t-16 p/array-t-32 
                
                ))

(defun box-pointer (base changed pointer)
  (let ((tag (logand pointer +tag-bits+)))
    (%box-pointer base changed pointer tag)))


;; (box-pointer #(0) #(nil) 123)
;; (box-pointer nil nil (pstring nil "David"))
;; (with-new-heap #(0) (box-pointer (pmake-array 100)))

;; then I can do this...

(defmethod print-object ((x boxed-pheap-object) (s stream))
  (print-object (pobject->lisp-object (base-vector x)
                                      (changed-blocks x)
                                      (pointer x))
                s))

(defmethod print-object ((x p/cons) (s stream))
  (write-char #\( s)
  (with-slots (base-vector changed-blocks)
      x
    (labels ((print-list (cons)
               (let ((car (pcar base-vector changed-blocks cons))
                     (cdr (pcdr base-vector changed-blocks cons)))
                 (print-object (box-pointer base-vector
                                            changed-blocks
                                            car)
                               s)
                 ;; how to print depends on what the cdr is...
                 (cond ((eql cdr p/nil))
                       ((eql (logand +tag-bits+ cdr)
                             p/cons)
                        (write-sequence " " s)
                        (print-list cdr))
                       (t
                        (write-sequence " . " s)
                        (print-object (box-pointer base-vector
                                                   changed-blocks
                                                   cdr)
                                      s))))))
      (print-list (pointer x))))
  (write-char #\) s)
  ;; Is this suppose to be here? At start?
  (terpri s))

(defun print-parray (x s)
  (write-sequence "#(" s)
  ;; now map over the array. I should have an efficient way of doing that...
  (let ((base (base-vector x))
        (changed (changed-blocks x)))
    (loop for i from 0 to (1- (plength base changed (pointer x)))
       do
         (unless (eql i 0) (write-sequence " " s))
         (print-object (box-pointer base changed
                                    (parray-ref base changed (pointer x) i))
                       s)))
  (write-sequence ")" s))

;; I could augment this with length etc
(defmethod print-object ((x p/array-t-16) (s stream))
  (print-parray x s))

(defmethod print-object ((x p/array-t-32) (s stream))
  (print-parray x s))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Hash table precursor...

(defun make-hash-table-precursor (w)
  (pmake-array w #x100001))

(declaim (inline phash))
(defun phash (key)
  (logand (sxhash key) #xfffff))

(declaim (inline hash-table-list))
(defun hash-table-list (b c table key &optional (hash (phash key)))
  (let ((r (parray-ref b c
                       table hash)))
    (if (= r 0) ; 0 can't be a valid list
        p/nil
        r)))

(defmacro ppush (w b c value place)
  (let ((current (gensym "current")))
    `(let ((,current (,(first place) ,b ,c ,@ (cdr place))))
       (when (= ,current 0)
         (setf ,current p/nil))
       (setf (,(first place) ,w ,@ (cdr place))
             (pcons w ,value ,current)))))

;; This isn't really a hash table since it doesn't store the keys
(declaim (inline add-to-hash-table))
(defun add-to-hash-table (w b c table key object)
  (let ((hash (phash key)))
    (ppush w b c object
           (parray-ref table hash))))

;; where the objects must be kept sorted
;; I hope this doesn't slow things down too much
(defun add-to-sorted-hash-table (w b c table key object)
  ;; !!! Enable tail calls for this sorting
  (declare (optimize (debug 1)))
  (let* ((hash (phash key))
         (r (parray-ref b c table hash)))
    (when (= r 0) (setf r p/nil))
    ;; (format t "add-to-sorted-hash-table ~A ~A ~A~%" table key object)
    ;; now we have to generate a (maybe new) list of the new object + the others but sorted...
    (labels ((unwind (earlier list)
               (if earlier
                   (unwind (cdr earlier)
                           (pcons w (car earlier)
                                  list))
                   list))
             (new-sorted-list (earlier object list)
               (if (= list p/nil)
                   (unwind earlier (pcons w object p/nil))
                   (let ((next (pcar b c list)))
                     ;; (format t "Collision of ~A~%" object)
                     (cond ((> object next)
                            (unwind earlier (pcons w object list)))
                           ((= object next)
                            (unwind earlier list))
                           (t (new-sorted-list (cons next earlier)
                                               object
                                               (pcdr b c list))))))))
      (setf (parray-ref w table hash)
            (new-sorted-list nil object r)))))

