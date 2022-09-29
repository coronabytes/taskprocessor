# .NET Background Task Processing Engine
*Hangfire Pro Redis* except:
- way less features
- open source (Apache 2.0)
- only redis 6.0+ as a storage engine
- low level api (bring your own serialization / ui)
- multi tenancy
- global pause and resume of all processing
- every task belongs to a batch
- batch cancelation can abort in process tasks "instantly"
- easy to access batch statistics per tenant
- async extension points with access to batch information
