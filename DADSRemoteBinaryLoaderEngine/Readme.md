Here is a proposal for a workload

```racket
(EvaluateInEngines
 (List "libDADSRemoteBinaryLoaderEngine.so")
 (Set OSMData (LOAD "http://columndata")))

(EvaluateInEngines
 (List "libDADSRemoteBinaryLoaderEngine.so")
 (Select 'OSMData (Where (Greater Length 17))))

;; should yield this: (Select (Table (beginID (List 5 9 1)) (endID (List 5 1 9)) (length (List 2.5 1.9 0.7)) ))



(EvaluateInEngines
 (List "libDADSRemoteBinaryLoaderEngine.so" "VolcanoEngine.so")
 (Top
  (Select
   (Join
    (Join
     (Project 'OSMData (As FirstBegin beginID) (As FirstEnd endID) (As FirstLength length))
     (Project 'OSMData (As SecondBegin beginID) (As SecondEnd endID) (As SecondLength length))
     (Where (Equal FirstEnd SecondBegin)))
    (Project 'OSMData
             (As ThirdBegin beginID)
             (As ThirdEnd endID)
             (As ThirdLength length))
    (Where (And (Equal SecondEnd ThirdBegin) (Equal ThirdEnd FirstBegin))))
   (Where (Greater (Plus FirstLength SecondLength ThirdLength) 17))
   )
  10 (Multiply FirstLength SecondLength ThirdLength)
  ))```
