# A Domain Specific Language to support the integration of Machine Learning analysis into IoT-Enhanced Business Processes

The integration of Machine Learning into IoT-Enhanced Business Processes (BPs) offers significant opportunities to enable predictive and adaptive process behavior. However, in current practice, ML capabilities are typically developed through ad-hoc pipelines that remain disconnected from process modeling and execution, leading to limited traceability, maintainability, and reuse. To address this challenge, this work proposes a Domain-Specific Language (DSL) that supports the systematic and model-driven integration of ML into IoT-Enhanced BPs. The DSL enables the explicit specification of heterogeneous data sources, feature engineering operations, ML-ready datasets, and ML model configurations within a unified and platform-independent framework. The approach is supported by a runtime interpreter for automatic dataset construction and a model-to-code transformation that generates executable ML pipelines.

The DSL was evaluated through a usability study involving participants with expertise in BPM, IoT, and ML, as well as through the validation of the supporting tools. The results show that the DSL is usable enough to create predictive artifacts in the context of IoT-Enhanced BPs and the supporting tools automatically generate correct and completed software artifacts. This work contributes to advancing model-driven approaches for integrating ML into IoT-Enhanced BPs, paving the way toward more integrated, traceable and maintainable solutions.

# Folders

The content of the folders is the following:

* "DSL" contains (1) the JSON Schema that defines the structural grammar of the DSL and (2) the artefacts produced in a usability experiment of the DSL.
* "Dataset Runtime Interpreter" contains: (1) the Java source code of the software infrastructure that interprets DSL definitions in order to generate ML-ready datasets, and (2) the artefacts used to validate this infrastructure.
* "DSL2Python Transformation" contains the Java source code that implementes a Model-to-Code transformation that generates ML Python algorithms from DSL specifications. The test folder include the JUnit testing performed to this transformation.