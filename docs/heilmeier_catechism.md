Jack's IRAD (Next-Gen Anomaly Detection) 
1. What are you trying to do? Articulate your objectives using absolutely no jargon.
   - Develop a new anomaly detection framework for multi-rate, mixed-type, multivariate telemetry timeseries
2. How is it done today, and what are the limits of current practice?
   - Current MADI implementation trains binary classifier (deep neural net) with synthetic anomalies (uniform random distributed examples drawn from bounded feature superspace) and computes integrated gradients for sensor blame attribution. While this modeling approach benefits from the blessing of dimensionality in principle, it pushes up against engineering constraints at scale. Integrated gradients and neural networks are both computationally heavy, and current implementation uses heavy ETL processing. Moreover, current model semantics lacks system hierarchy, temporal dynamics, sensor types and behavior, flight phase, etc.  
3. What is new in your approach and why do you think it will be successful?
   - The new approach was developed from the ground up to address engineering requirements: 
     - process fleet-scale A-MATS telemetry (30k parameter, high-refresh, mixed-type, multi-rate, 10 Gb/flt-hr, 100 TB/yr)
       - linear transformations
       - JVM hot path
       - cutting-edge data reduction techniques 
   - Independent of SME knowledge
     - data-driven discovery  sensor hierarchy
     - datatype, parameter and behavior profiling
     - data-driven phase-of-flight discovery    
   - First-class dynamical features
     - event-based ontology
     - sensor behavior
     - system couplings
   - multiple anomaly detection channels
     - sensor misbehavior
     - coupling misbehavior
     - reconstruction loss
     - graph violations
   - Calibrated anomaly scoring (real-probabilities, conditioned on phase-of-flight)
   - hierarchical global\system\subsystem\module\parameter anomaly attribution down to the event timestamp     
   - Extensible, realistic simulation engine for sensor system digital twin development and testing 
4. Who cares? If you are successful, what difference will it make?
   - The project will deliver fast, reproducible, linear, fleet-scale processing of next-gen avionics telemetry for explainable anomaly detection with deep model semantics. The product is designed to operate in War Data Platform Databricks or any spark cluster. It is intended to dovetail other eRCM and CBM+ efforts at Redhorse for current and prospective customers
5. What are the risks?
   - Independent R&D project until maturity (bus factor)
   - general AI uncertainty
   - risk is low, project is focused on concrete Redhorse objectives
6. How much will it cost?
   - 1 IC (Jack) at 25% time (suggested)
   - Claude Code and OpenAI account
     - token limit increase (faster progress) 
7. How long will it take?
What are the mid-term and final “exams” to check for success?
 - Expected outcome
   - mature code repo
     - 1-2 mo. out
   - deployment in Advana against C130J aircat dataset
     - 2 mo. out
   - Demo
     - 3 mo. out
   - Academic research article draft
     - 4 mo. out  
   - SBIR proposal (Technical Volume draft)
     - 5 mo. out
    