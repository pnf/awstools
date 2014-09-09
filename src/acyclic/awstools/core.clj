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

(defn start [is]
  (let [t (start-instances :instance-ids is)]
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
   " --message `(echo \"id: " id "\";bin/ec2-metadata -i -p -o) | base64 -w 0`"
   "\n"))

;(stop-instances :instance-ids ["i-7c949193"])
;(modify-instance-attribute :instance-id "i-7c949193" :user-data (s->b64 "echo hello"))

(defn good-strings [vs]
  (and (sequential?  vs)
       (every? string? vs)
       (every? pos? (map count vs))))



(defn- really-up? [rs->info rs]
  (let [info (get rs->info rs)]
    (and
     (= (:state info) "running")
     (string? (:ip info))
     (pos? (count (:ip info))))))

(defn bring-up-spots  [reqmap nmin cmds & opts]
  (let [id  (.toString (UUID/randomUUID))
        cmd (str (send-up id) (clojure.string/join "\n" cmds) "\n")
        req (apply ph-assoc reqmap paths opts)
        req (ph-assoc req paths :udata cmd)
        n   (ph-get reqmap paths :n)
        req (if (<= nmin n) req (ph-assoc req paths :n nmin))
        _   (debug "Requesting:" id req)
        cl  (notify-chan id)
        rs  (request-spots req)
        to  (timeout (* 5 60 1000))]
    (debug "Spot requests:" rs)
    (async/go-loop [i 0]
      (if (>= i nmin)
        (let [rs->info (request-info-map rs)
              pred     (partial really-up? rs->info)
              rs-up    (filter pred rs)
              rs-dn    (filter (complement pred) rs)]
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

(defn bring-up-instances [is]
  (let [n  (count is)
        id (.toString (UUID/randomUUID))
        cl (notify-chan id)
        to (timeout (* 5 60 1000))]
    (doseq [i is]
            (modify-instance-attribute :instance-id i :user-data (s->b64 (send-up id))))
    (start is)
    (async/go-loop [i n]
      (if (zero? i)
        (instance-info-map is)
        (let [[v c] (async/alts! [cl to])]
          (cond
           (= c to) (do (info "Timeout on instance request:" is)
                        (close-notify-chan! id)
                        (stop is)
                        nil)
           (= c cl) (recur (dec i))))))))


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



