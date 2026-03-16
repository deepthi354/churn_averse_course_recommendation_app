# Churn Averse Course Recommendation App

## Project Overview 
A recommendation engine built using Markov chain transition probabilities to maximize learner engagement and lifetime value while minimizing churn.

*Applied following techniques:* 
- Markov-Chain Transition Logic to prioritize courses with lower churn & higher course transition probabilities instead of the standard 'Those who take X also take Y' content/user similarity. 
- Eliminated noise, such as accidental clicks & exploratory behaviors, by modelling on recent 6 months data of high intent engaged learning pathways (>50% learning progress on short courses & >25% learning progress on long form courses).
- 3 recommendations are personalized to match user's skill preference & learning capability (2 same difficulty level & 1 higher) while accounting for lowest churn, so that learner does not feel disoriented/down-leveled/overwhelmed.
- 1 recommendation is targeted to gamify using the 'Fastest Path of Least Resistance to master a skill' & create a sense of psychological win.  
- Skill Profile Visualization (Radar Chart) to ease discovery of unexplored categories & motivate learners to chase for expertise.

## Tech Stack
- **Frontend:** HTML5, tailwind CSS, JavaScript (plotly-2.24.1.min.js For radar chart rendition)
- **Backend (Logic):** AWS Lambda (Python functions), API Gateway (For API request/response)
- **Data:** AWS Redshift for realtime data (static sample json files for demo purpose)
- **Storage:** AWS S3
