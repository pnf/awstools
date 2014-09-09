(ns acyclic.awstools.core
  (:use [amazonica.core]
        [amazonica.aws.ec2]
        [amazonica.aws.sqs]
        acyclic.utils.pinhole
        acyclic.utils.log)
  (:require [clj-ssh.ssh :as ssh]
            [taoensso.timbre :as timbre]
            [clojure.data.codec.base64 :as b64]
            [clojure.data.json :as json]
            [clojure.core.async.impl.protocols :as pimpl]
            [clojure.core.async :as async 
             :refer [<! >! <!! >!! timeout chan alt!! go close!]])
  (:import (java.util UUID)))
(timbre/refer-timbre)


(def cred (read-string (slurp "AWS.clj")))

(apply defcredential (map cred [:access-key :secret-key :endpoint]))

(defn s->b64 [s] (String. (b64/encode (.getBytes s))))

(defn b64->s [s] (String. (b64/decode (.getBytes s))))

(defn- m->pair [[_ k v]] [(keyword k) v])

(defn b64->ec2-data [s]
  (let [xs  (-> s b64->s clojure.string/split-lines)
        es  (map #(->> % 
                       (re-matches #"^([\w\-]+):\s*(.*)")
                       m->pair) xs)]
    (into {} es)))

(defn chan->seq
  "Drains a channel onto a lazy sequence.  Blocks internally."
  [c & [terminate?]]
  (lazy-seq
   (when-let [v (if terminate?
                  (first (async/alts!! [c] :default nil))
                  (<!! c))]
     (cons v (chan->seq c terminate?)))))


(def ^{:private true} paths {:zone  [:launch-specification :placement :availability-zone]
                             :itype [:launch-specification :instance-type]
                             :subnet [:launch-specification :instance-network-interface-specification 0 :subnet-id]
                             :group [:launch-specification :network-interfaces 0 :groups 0]
                             :public? [:launch-specification :network-interfaces 0 :associate-public-ip-address]
                             :price [:spot-price]
                             :n     [:instance-count]
                             :udata [s->b64 :launch-specification :user-data]})


(def my-ec2-info nil)
(def my-req nil)
(def my-sns-topic nil)
(def my-sqs-url nil)
(def my-region "us-east-1")
(defn slurp-ec2-info [f]
  (let [i (read-string (slurp f))]
    (alter-var-root (var acyclic.awstools.core/my-ec2-info) #(do % i))
    (alter-var-root (var acyclic.awstools.core/my-req) #(do % (:req i)))
    (alter-var-root (var acyclic.awstools.core/my-region) #(do % (:region i)))
    (alter-var-root (var acyclic.awstools.core/my-sns-topic) #(do % (:sns-topic i)))
    (alter-var-root (var acyclic.awstools.core/my-sqs-url) #(do % (:sqs-url i))))
  nil
  )


(defn cmd-timeout [sec cmd & args]
  (let [cc (go (apply cmd args))
        to (timeout (* 1000 sec))
        [v c]     (async/alts!! [cc to])]
    (if (= c cc) v
      (do (debug "cmd" args "timed out after" sec) nil))))

(defn sqs-listen
  "Listen on an SQS queue, sending messages to a channel, which is returned."
 [& [url]]
  (let [url (or url my-sqs-url)
        c   (chan)]
    (async/go-loop []
      (if (pimpl/closed? c)
        (debug "Shutting down sqs-listen" url)
        (let [_ (trace "sqs-listen polling" url)
              messages  (:messages (receive-message :queue-url url :wait-time-seconds 20))]
          (trace "sq-listen" url "received" messages)
          (doseq [{r :receipt-handle b :body} messages]
            (debug "sqs-listen received" b)
            (try (>! c (get (json/read-str b) "Message"))
                 (catch Exception e (info "sqs-listen" (stack-trace e))))
            (try (delete-message :queue-url url :receipt-handle r)
                 (catch Exception e (info "sqs-listen" (stack-trace e)))))
          (recur))))
    c))

(defmacro go-try [& forms]
  `(go (try (do ~@forms)
            (catch Exception e#
              (do (debug "Caught exception" e#) nil)))))

(defn request-status [rs]
  (let [d    (cmd-timeout 5 describe-spot-instance-requests :spot-instance-request-ids rs)
        sirs (:spot-instance-requests d)
        _    (debug "Request statuses" rs d sirs)]
    (map :state sirs)))

(defn instance-info-map
  "Returns map of instance-id to map of information about instance."
  [is]
  (let [res   (:reservations (describe-instances :instance-ids is))
        ims   (map #(get-in % [:instances 0]) res)
        xms   (map (fn [im] (m-section im
                      [:instance-id
                       [:state [:state :name]]
                       [:ip :private-ip-address]
                       [:host [:public-dns-name #(when (seq %) %)]]])) ims)]
    (mseq->m xms :instance-id)))

(defn request-info-map [ids]
  (let [descs  (describe-spot-instance-requests :spot-instance-request-ids ids)
        sirs   (:spot-instance-requests descs)
        sirs   (map (fn [rm] (m-section rm [[:request-id :spot-instance-request-id] :state :instance-id])) sirs)
        is     (filter string? (map :instance-id sirs))
        ims    (instance-info-map is)
        sirs   (map #(merge % (ims (:instance-id %))) sirs)]
    (mseq->m sirs :request-id)))




(defn good-strings [vs]
  (and (sequential?  vs)
       (every? string? vs)
       (every? pos? (map count vs))))

(defn request-instances [rs]
  (let [d (describe-spot-instance-requests :spot-instance-request-ids rs)]
    (map :instance-id (:spot-instance-requests d))))

(defn request-spots [req & opts]
  (let [req    (apply ph-assoc req paths opts)
        args   (apply concat (seq req))
        _      (debug "Spot request" (pr-str args))
        rs     (apply request-spot-instances args)]
    (map :spot-instance-request-id  (:spot-instance-requests rs))))

(defn terminate [is]
  (let [t (terminate-instances :instance-ids is)]
    (debug t)
    t))

(defn stop [is]
  (let [t (stop-instances :instance-ids is)]
    (debug t)
    t))

(defn cancel-requests [rs]
  (let [ds (:spot-instance-requests (describe-spot-instance-requests :spot-instance-request-ids rs))
        is (filter (complement nil?) (map :instance-id ds))
        cr (:cancelled-spot-instance-requests (cancel-spot-instance-requests :spot-instance-request-ids rs))
        ci (and (seq is) (terminate is))]
    [cr ci]))


(def ids->chs (atom {}))

(defn notify-chan [id]
  (let [c (chan)]
    (swap! ids->chs assoc id c)
    c))

(defn close-notify-chan! [id]
  (swap! ids->chs #(let [c (get % id)]
                     (close! c)
                     (dissoc % id)))
  nil)


(defn start-up-listener []
  (let [ctl  (chan)
        cl   (sqs-listen)]
    (swap! ids->chs #(assoc % :sqs-listen cl))
    (async/go-loop []
      (let [[v c] (async/alts! [ctl cl])]
        (cond
         (= c ctl) (do (reset! ids->chs {}) (close! cl))
         (= c cl)  (do (try 
                          (let [s   (b64->ec2-data v)
                                id  (:id s)
                                c   (get @ids->chs id)]
                            (debug "Got" id c s)
                            (when c (>! c s)))
                          (catch Exception e (info e)))
                       (recur)))))
    ctl))


;;aws --region us-east-1 sns publish --topic-arn  arn:aws:sns:us-east-1:633840533036:instance-up --message yowsassl

(defn send-up [id & {topic :topic region :region}]
  (str
   "aws --region " (or region my-region)
   " sns publish --topic-arn " (or topic my-sns-topic)
   " --message `(echo \"id: " id "\";bin/ec2-metadata -i -p -o) | base64 -w 0`"))

;(stop-instances :instance-ids ["i-7c949193"])
;(modify-instance-attribute :instance-id "i-7c949193" :user-data (s->b64 "echo hello"))


(defn bring-up-spots  [reqmap nmin cmds & opts]
  (let [cmd (str (send-up) "\n "(clojure.string/join "\n" cmds))
        req (apply ph-assoc reqmap paths opts)
        req (ph-assoc req paths :udata cmd)
        ;req (assoc req :nmin nmin)
        n   (ph-get reqmap paths :n)
        req (if (<= nmin n) req (ph-assoc req paths :n nmin))
        id  (.toString (UUID/randomUUID))
        _   (debug "Requesting:" id req)
        cl  (notify-chan id)
        rs  (request-spots req)
        to  (timeout (* 5 60 1000))]
    (debug "Spot requests:" rs)
    (async/go-loop [i 0]
      (if (>= i nmin)
        (let [rs->info (request-info-map rs)
              rs-up    (filter rs->info rs)
              rs-dn    (filter (complement rs->info) rs)]
          (close-notify-chan! id)
          (cancel-requests rs-dn)
          (map rs->info rs-up))
        (let [[v c] (async/alts! [cl to])]
          (cond
           (= c to) (do (info "Timeout on spot request:" req)
                      (close-notify-chan! id)
                      (cancel-requests rs)
                      nil)
           (= c cl) (recur (inc i))))))))


(def ag (ssh/ssh-agent {}))

(defn ssh-session [host]
  (let [sess (ssh/session ag host {:strict-host-key-checking :no
                                   :username "ec2-user"})]
    (<!! (async/go-loop [n 10]
           (cond
            (ssh/connected? sess) sess
            (zero? n) (do (debug "Failed to connect to" host) nil)
            :else     (do
                        (try (ssh/connect sess)
                             (catch Exception e (debug "Attempt" n "failed:" e)))
                        (if (ssh/connected? sess)
                          (do
                            sess)
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




(defn EDNify
"EDN-ify an arbitrary object, leaving it alone if it's an innocuous string."
  [x] 
  (let [x (if (string? x) x (pr-str x))
        x (if (re-matches #"[0-9a-zA-z-_\.]+" x) x (pr-str x))]
    x))


(defn commandify
  "If cmd is a sequence, convert it into a space-delimited string, EDNifying as necessary."
  [cmd]
  (cond (string? cmd) (clojure.string/trim cmd)
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


#_(defn request-info [ids]
  (let [d      (cmd-timeout 5 describe-spot-instance-requests :spot-instance-request-ids (filter #(re-find #"^sir-" %) ids))
        sirs   (:spot-instance-requests d)
        rs     (map :spot-instance-request-id sirs)
        rs->rs (into {} (map #(vector %1 %2) rs0 rs))
        ss     (map :state sirs)
        is     (map :instance-id sirs)
        rs->i  (into {} (map #(vector %1 %2) rs is))
        is-up  (filter identity is)
        is-r   (:reservations (describe-instances :instance-ids is-up)) 
        is-i   (map #(get-in % [:instances 0]) is-r)
        ;ip-pub (map :public-ip-address is-i)
        is-up  (map :instance-id is-i)
        ip-pri (map :private-ip-address is-i)
        hosts  (map :public-dns-name is-i)
        hosts  (map #(when (seq %) %) hosts)
        i->inf (into {} (map #(vector %1 {:instance %1 :ip %2 :host %3}) is-up ip-pri hosts))
        ]
    (map #(merge {:request %1 :status %2} ((comp i->inf rs->i) %1)) rs0 ss)))


#_(defn patience
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


#_(defn pluck [c]
  (let [a (atom nil)]
    (close! (go (let [x (<! c)]
                  (reset! a x))))
    a))


#_(defn- add-sessions [req]
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

#_(defn chain-some
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


;; 
#_(defn bring-up-aws
  "Bring up n AWS t1.micro instances and return channel that will contain a single map
of {:instance ids :hosts names and :sessions objects}, with appropriate timeouts.
Returns a channel that will contain a map of :hosts, :sessions, etc or nil."
  [reqmap nmin & opts]
  (let [req (apply ph-assoc reqmap paths opts)
        req (assoc req :nmin nmin)
        n    (ph-get reqmap paths :n)
        req (if (<= nmin n) req (ph-assoc req paths :n nmin))]
    (debug "Bringing up" req)
    (chain-some {:req req} add-requests; add-instances add-ips add-hosts
                )
    ))
;; add-sessions



#_(defn- add-requests [req]
  (let [req (assoc req :rs (request-spots (:req req)))]
    (patience
     #(assoc req :req-status (cmd-timeout 5 request-status (:rs req)))
     (fn [req] (let [rss (:req-status req)]
                 (and (seq rss) (every? #(= "active" %) rss))))
   #(give-up-and-cancel req "Failed to fulfill spot requests")
   600 60)))

#_(defn- add-instances [req]
  (patience 
   #(assoc req :is (request-instances (:rs req)))
   #(good-strings (:is %))
   #(give-up-and-cancel req "Failed to get instance ids")
   10 1))


#_(defn- add-hosts [req]
  (patience
   (fn [] (assoc req :hosts (->> (:is req)
                                 (describe-instances :instance-ids)
                                 :reservations
                                 (map #(get-in % [:instances 0 :public-dns-name])))))
   #(good-strings (:hosts %))
   #(give-up-and-cancel req "Failed to get hosts.")
   10 1))


#_(defn- add-ips [req]
  (patience
   (fn [] (assoc req :ips (->> (:is req)
                                 (describe-instances :instance-ids)
                                 :reservations
                                 (map #(get-in % [:instances 0 :private-ip-address])))))
   #(good-strings (:ips %))
   #(give-up-and-cancel req "Failed to get IPs")
   10 1))

#_(defn- give-up-and-cancel [req & [msg]]
  (when msg (debug msg))
  (concat  (some-> req :is terminate)
           (some-> req :rs cancel)))

