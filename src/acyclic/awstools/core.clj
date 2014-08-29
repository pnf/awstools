(ns acyclic.awstools.core
  (:use [amazonica.core]
        [amazonica.aws.ec2]
        acyclic.utils.pinhole)
  (:require [clj-ssh.ssh :as ssh]
            [taoensso.timbre :as timbre]
            [clojure.data.codec.base64 :as b64]
            [clojure.core.async :as async 
             :refer [<! >! <!! >!! timeout chan alt!! go close!]]))
(timbre/refer-timbre)

; ami-5cd51634



(def cred (read-string (slurp "AWS.clj")))

(apply defcredential (map cred [:access-key :secret-key :endpoint]))

(defn s->b64 [s] (String. (b64/encode (.getBytes s))))


(def ^{:private true} paths {:zone  [:launch-specification :placement :availability-zone]
                             :itype [:launch-specification :instance-type]
                             :price [:spot-price]
                             :n     [:instance-count]
                             :udata [s->b64 :launch-specification :user-data]})


(defmacro go-try [& forms]
  `(go (try (do ~@forms)
            (catch Exception e#
              (do (debug "Caught exception" e#) nil)))))

(defn patience
  "Invoke (genfn) every time-wait sec until predicate returns true or time-limit sec has passed.
Returns result of genfn, or nil in the case of timeout."
  [genfn pred failure time-limit time-wait]
  (let [t1     (timeout (* 1000 time-limit))
        out    (chan)]
    (async/go-loop []
      (let [rc     (go-try (genfn))
            [v c]  (async/alts! [rc t1 out])]
        (debug "Ats:" v c)
        (cond
         (= c out)                    (do (debug "Canceled")
                                          (when failure (failure)))
         (= c t1)                     (do (debug time-limit "sec expired")
                                          (when failure (failure))
                                          (close! out))
         (and (= c rc) v (pred v))    (do (debug "Returning" v)
                                          (>! out v))
         :else                        (do (debug "Still" v "Waiting" time-wait)
                                          (<! (timeout (* 1000 time-wait)))
                                          (debug "Woke up after wait...")
                                          (recur)))))
    out))



(defn pluck [c]
  (let [a (atom nil)]
    (close! (go (let [x (<! c)]
                  (reset! a x))))
    a))


(defn request-spots [req]
  (let [req    (apply concat (seq req))
        _      (debug req)
        rs     (apply request-spot-instances req)]
    (map :spot-instance-request-id  (:spot-instance-requests rs))))

(defn cmd-timeout [sec cmd & args]
  (let [cc (go (apply cmd args))
        to (timeout (* 1000 sec))
        [v c]     (async/alts!! [cc to])]
    (if (= c cc) v
      (do (debug "cmd" args "timed out after" sec) nil))))

(defn request-status [rs]
  (let [d    (cmd-timeout 5 describe-spot-instance-requests :spot-instance-request-ids rs)
        sirs (:spot-instance-requests d)
        _    (debug "Request statuses" rs d sirs)]
    (map :state sirs)))


(defn good-strings [vs]
  (and (sequential?  vs)
       (every? string? vs)
       (every? pos? (map count vs))))

(defn request-instances [rs]
  (let [d (describe-spot-instance-requests :spot-instance-request-ids rs)]
    (map :instance-id (:spot-instance-requests d))))

(defn cancel [rs]
  (let [c (:cancelled-spot-instance-requests
        (cancel-spot-instance-requests :spot-instance-request-ids rs))]
    (debug c)
    c))

(defn terminate [is]
  (let [t (terminate-instances :instance-ids is)]
    (debug t)
    t))

(defn- give-up-and-cancel [req & [msg]]
  (when msg (debug msg))
  (concat  (some-> req :is terminate)
           (some-> req :rs cancel)))

(defn request-spots [req]
  (let [args    (apply concat (seq req))
        rs     (apply request-spot-instances args)]
    (debug "Spot request" args "->" rs)
    (map :spot-instance-request-id  (:spot-instance-requests rs))))

(defn- add-requests [req]
  (let [req (assoc req :rs (request-spots (:req req)))]
    (patience
     #(assoc req :req-status (cmd-timeout 5 request-status (:rs req)))
     (fn [req] (let [rss (:req-status req)]
                 (and (seq rss) (every? #(= "active" %) rss))))
   #(give-up-and-cancel req "Failed to fulfill spot requests")
   600 60)))

(defn- add-instances [req]
  (patience 
   #(assoc req :is (request-instances (:rs req)))
   #(good-strings (:is %))
   #(give-up-and-cancel req "Failed to get instance ids")
   10 1))


(defn- add-hosts [req]
  (patience
   (fn [] (assoc req :hosts (->> (:is req)
                                 (describe-instances :instance-ids)
                                 :reservations
                                 (map #(get-in % [:instances 0 :public-dns-name])))))
   #(good-strings (:hosts %))
   #(give-up-and-cancel req "Failed to get hosts.")
   10 1))


(defn- add-ips [req]
  (patience
   (fn [] (assoc req :ips (->> (:is req)
                                 (describe-instances :instance-ids)
                                 :reservations
                                 (map #(get-in % [:instances 0 :private-ip-address])))))
   #(good-strings (:ips %))
   #(give-up-and-cancel req "Failed to get IPs")
   10 1))

(def ag (ssh/ssh-agent {}))

(defn ssh-session [host]
  (let [sess (ssh/session ag host {:strict-host-key-checking :no
                                   :username "ec2-user"})]
    (<!! (async/go-loop [n 10]
           (cond
            (ssh/connected? sess) sess
            (zero? n) (do (debug "Failed to connect to" host) nil)
            :else     (do
                        (ssh/connect sess)
                        (if (ssh/connected? sess)
                          sess
                          (do
                            (<! (timeout 1000))
                            (recur (dec n))))))))))


(defn ssh-sessions [hosts]
  (let [sess (map #()
                  hosts)]
    (loop [n 10]
           (let [o (map #(or (ssh/connected? %) (ssh/connect %)) sess)]
             (if (or (every? o)
                     (zero? n)) sess
                 (do 
                   (Thread/sleep 1000)
                   (recur (dec n))))))))




(defn- add-sessions [req]
  (let [sess (map #(ssh/session ag % {:strict-host-key-checking :no
                                      :username "ec2-user"})
                  (:hosts req))]
    (patience
     (fn [] (let [sbad (filter #(not (ssh/connected? %)) sess)]
              (doseq [s sbad] (ssh/connect s))
              (assoc req :sessions sess)))
     #(every? ssh/connected? (:sessions %1))
     nil ;#(give-up-and-cancel req "Failed to add sesions")
     60 10
     )))

(defn chain-some
  "Each cfn must be a function of one parameter, returning a channel that will deliver
its non-nil result, or nil on failure.  Non-nil return values will be progressively passed
to each cfn in order.  Returns a channel that will contain the result of the final
cfn or nil, if one of them failed along the way."
  [x & cfns]
  (let [out (chan)]
    (debug "Starting" cfns)
    (async/go-loop [[f & fs] cfns
                    x        x]
      (if-not f
        (do (debug "Finished" cfns)
            (>! out x))
        (let [_ (debug "Invoking" f x)
              y (<! (f x))]
          (if-not y
            (do (debug "Failed at" f)
                (close! out))
            (recur fs y)))))
    out))


(defn bring-up-aws
  "Bring up n AWS t1.micro instances and return channel that will contain a single map
of {:instance ids :hosts names and :sessions objects}, with appropriate timeouts.
Returns a channel that will contain a map of :hosts, :sessions, etc or nil."
  [reqmap nmin & opts]
  (let [req (apply ph-assoc reqmap paths opts)
        req (assoc req :nmin nmin)
        n    (ph-get reqmap paths :n)
        req (if (<= nmin n) req (ph-assoc req paths :n nmin))]
    (debug "Bringing up" req)
    (chain-some {:req req} add-requests add-instances add-ips add-hosts)
    ))
;; add-sessions

(defn EDNify
"EDN-ify an arbitrary object, leaving it alone if it's an innocuous string."
  [x] 
  (let [x (if (string? x) x (pr-str x))
        x (if (re-matches #"[0-9a-zA-z-_\.]+" x) x (pr-str x))]
    x))


(defn commandify
  "If cmd is a sequence, convert it into a space-delimited string, EDNifying as necessary."
  [cmd]
  (cond (string? cmd) cmd
        (seq cmd) (clojure.string/join " " (map EDNify cmd))))

(defn ex
  "Make sure the session is connected and run the command remotely via
ssh-exec, yielding a map of :exit code, :out string and :err string."
  [sess cmd]
  (or (ssh/connected? sess) (ssh/connect sess))
  (ssh/ssh-exec sess (commandify cmd) "" "" {}))

(defn ex-async [sess cmd]
  "As ex, but returns a channel that will contain the map."
  (or (ssh/connected? sess) (ssh/connect sess))
  (let [c (chan)]
    (go (let [cmd (commandify cmd)
              _   (debug "Running in" sess cmd)
              req (ssh/ssh-exec sess cmd "" "" {})]
          (debug "Returning from" sess req)
          (>! c req) (close! c)))
    c))

(comment 
(def my-subnets {"us-east-1c"  "subnet-08eff44e"
                 "us-east-1a"  "subnet-f290abda"
                 "us-east-1b"  "subnet-572dd420"})
(def my-zone    "us-east-1a")
(def my-req {:spot-price 		0.01
             :instance-count 		1
             :type 			"one-time"
             :launch-specification     {:image-id 	       "ami-5cd51634"
                                        :instance-type 	       "t1.micro"
                                        :placement             {:availability-zone my-zone}
                                        :key-name	       "telekhine"
                                        :security-groups-ids   ["sg-78deaf1d"]
                                        :subnet-id             (get my-subnets my-zone)
                                        :iam-instance-profile  {:arn "arn:aws:iam::633840533036:instance-profile/girder-peer"}}})


)


#_(defn dns-names [is]
  (loop [n 5]
    (let [ds    (:reservations (describe-instances :instance-ids is))
          names (map #(get-in % [:instances 0 :public-dns-name]) ds)]
      (cond (or
           (and (seq names)
                (= (count names) (count is))
                (every? string? names))
           (zero? n))
        names
        (zero? n)
        nil
        :else
        (do
          (Thread/sleep 10000)
          (recur (dec n)))))))

#_(defn request-spots [n & {:keys [zone itype price image key profile group subnet]}]
  (let [zone   (or zone "us-east-1c")
        itype  (or itype "t1.micro")
        price  (str (or price "0.005"))
        image  (or image "BLEH")
        sgs    (if (seq sg) sg [sg])
        req    [:spot-price 			price
                :instance-count 		n
                :type 			"one-time"
                :launch-specification 
                {:image-id 			image
                 :instance-type 		itype
                 :placement {:availability-zone      zone}
                 :key-name			key
                 :security-groups-ids	     sgs
                 :subnet-id                  (get my-subnets zone)
                 :iam-instance-profile       {:arn "arn:aws:iam::633840533036:instance-profile/girder-peer"}}]
        (apply request-spot-instances req)]
    (map :spot-instance-request-id  (:spot-instance-requests r))))

#_(defn patience
  "Invoke (genfn) every time-wait sec until predicate returns true or time-limit sec has passed.
Returns result of genfn, or nil in the case of timeout."
  [pred genfn time-limit time-wait]
  (let [t1     (if (number? time-limit) (timeout (* 1000 time-limit)) time-limit)
        out    (chan)]
    (async/go-loop []
      (let [r   (genfn)
            p   (pred r)]
        (if p
         (do (debug "Returning" r)
              (>! out r) (close! out))
         (let [t2     (timeout (* 1000  time-wait))
               _      (debug "Still" r "Waiting" time-wait "sec")
               [v c] (async/alts! [t1 t2])]
           (if (= c t1)
             (do 
               (debug time-limit "seconds expired.  Returning nil.")
               (close! out))
             (recur))))))
    out))


#_(defn patience-every [pred collfn time-limit time-wait]
  (patience #(every? pred %) collfn time-limit time-wait))


#_(defn- add-sessions [res]
  (let [out   (chan)
        tout  (timeout 60000)
        rs    (:rs res)
        hosts (:hosts res)
        sess  (map #(ssh/session ag % {:strict-host-key-checking :no
                                                  :username "ec2-user"})
                   hosts)]
    (async/go-loop []
      (if (every? ssh/connected? sess)
        (do (debug "All connected.  Returning.")
            (>! out (assoc res :sessions sess)))
        (let 
          (if (= c tout)
            (do
              (debug "Failed to establish connections after timeout.  Terminating.")
              (cancel rs)
              (close! out))
            (if #(every? ssh/connected? sess)
              (do (debug "Returning connected" sess)
                  (>! out (assoc res :sessions sess)))
              (do
                (debug "Waiting 10 sec")
                (<! (timeout 10000))
                (recur)))))))
    out))


#_(defn- add-requests [res]
  (let [req (:req res)
        out (chan)]
    (go 
      (let [rs  (request-spots req)]
        (debug "waiting for" rs "to be active")
        (<! (timeout 1000))
        (if (<! (patience-every #(= "active" %) #(request-status rs) 600 60))
          (do (debug "All instances active! Returning" (assoc res :rs rs))
              (>! out (assoc res :rs rs)))
          (give-up-and-cancel res out "Requests failed.")
)))
    out))
