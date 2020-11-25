
(in-package :cl-user)

(defpackage :cldb
  (:use :cl :anaphors :clim-internals)
  (:export #:query

           #:index-lookup
           #:lookup 
           #:column-equal
           #:column-equal
           #:column-satisfies
           #:get-column-value
           #:in
           #:not-in

           #:with-database

           #:collect
           #:collect-1
           #:collect-unique
           #:exists
           ;; #:with-database-transaction

           #:cldb-class
           #:get-instance
           #:with-database

           #:get-slot
           #:slot-equal
           #:slot-satisfies
           ))
