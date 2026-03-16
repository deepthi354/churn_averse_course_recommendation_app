import json
import boto3
import pandas as pd
import numpy as np
import io

# --- GLOBAL SCOPE AREA ---
# These lines run only ONCE when the Lambda container starts.
# They are now "cached" in memory for both Phase 1 and Phase 2.

# Initialize S3 client
s3 = boto3.client('s3')
BUCKET_NAME = 'dlai-data-static-site' 

def get_json_from_s3(file_key):
    """Fetches S3 object and returns as a dictionary/list"""
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    return json.loads(obj['Body'].read().decode('utf-8'))

# Load S3 Data (Handling both Records and Split formats)
df_courses = pd.DataFrame(get_json_from_s3('courses_data.json'))
df_skill_categories = pd.DataFrame(get_json_from_s3('skill_categories_data.json'))
SKILL_CATEGORY_COURSE_COUNT = df_skill_categories.groupby('skill_category').size().reset_index(name='course_count')

# lifetime_enrolls (split format)
lifetime_json = get_json_from_s3('lifetime_enrolls_completions_user_data.json')
df_lifetime_courses = pd.read_json(io.StringIO(json.dumps(lifetime_json)), orient='split')

# final_markov_matrix (split format)
markov_json = get_json_from_s3('markov_matrix_data.json')
final_markov_matrix = pd.read_json(io.StringIO(json.dumps(markov_json)), orient='split')



# --- 1. RANKING LOGIC (Expert Chase) ---
def category_ranking_logic(user_id, just_completed_course_id, target_category, 
                           df_lifetime_courses, completed_courses, 
                           final_markov_matrix, df_skill_categories, df_courses):
    
    category_courses = df_skill_categories[df_skill_categories['skill_category'] == target_category]['course_id'].unique().tolist()
    remaining_cat_courses = [c for c in category_courses if c not in completed_courses]
    
    cat_course_ids = df_skill_categories[df_skill_categories['skill_category'] == target_category]['course_id']
    valid_cat_courses = [cid for cid in cat_course_ids if cid in final_markov_matrix.index]
    
    if valid_cat_courses:
        cat_avg_churn = final_markov_matrix.loc[valid_cat_courses, 'CHURN'].mean()
    else:
        cat_avg_churn = final_markov_matrix['CHURN'].mean()

    cat_courses_probabilities_list = []
    for cid in remaining_cat_courses:
        churn_prob = final_markov_matrix.loc[cid, 'CHURN'] if cid in final_markov_matrix.index else cat_avg_churn
            
        trans_prob = 0
        if just_completed_course_id in final_markov_matrix.index:
            col_name = cid if cid in final_markov_matrix.columns else str(cid)
            if col_name in final_markov_matrix.columns:
                trans_prob = final_markov_matrix.loc[just_completed_course_id, col_name]
        
        cat_courses_probabilities_list.append({
            'course_id': cid,
            'churn_prob': churn_prob,
            'transition_prob': trans_prob
        })

    ranking_df = pd.DataFrame(cat_courses_probabilities_list)
    if ranking_df.empty: return ranking_df

    ranking_df = pd.merge(
        ranking_df, 
        df_courses[['course_id', 'course_name', 'difficulty_level', 'course_description']], 
        on='course_id', 
        how='left'
    )
    
    return ranking_df.sort_values(by=['churn_prob', 'transition_prob'], ascending=[True, False]).reset_index(drop=True)

# --- 2. RECOMMENDATION EXTRACTION ---
def extract_top_3_recommendations(ranking_df, just_completed_course_id, df_courses):
    diff_order = {'Beginner': 0, 'Intermediate': 1, 'Advanced': 2}
    inv_diff_order = {v: k for k, v in diff_order.items()}
    
    current_diff_str = df_courses.loc[df_courses['course_id'] == just_completed_course_id, 'difficulty_level'].values[0]
    current_val = diff_order.get(current_diff_str, 0)
    
    if current_val == 2:
        target_same_val, target_high_val, same_needed, high_needed = 2, 2, 3, 0
    else:
        target_same_val, target_high_val, same_needed, high_needed = current_val, current_val + 1, 2, 1

    final_selection = []
    if high_needed > 0:
        high_pool = ranking_df[ranking_df['difficulty_level'] == inv_diff_order[target_high_val]]
        high_selected = high_pool.head(high_needed)
        final_selection.append(high_selected)
    else:
        high_selected = pd.DataFrame()

    adj_same_needed = same_needed + (high_needed - len(high_selected))
    same_pool = ranking_df[ranking_df['difficulty_level'] == inv_diff_order[target_same_val]]
    final_selection.append(same_pool.head(adj_same_needed))
    
    current_count = sum(len(df) for df in final_selection)
    if current_count < 3:
        already_selected_ids = pd.concat(final_selection)['course_id'].tolist() if current_count > 0 else []
        for v in range(current_val - 1, -1, -1):
            lower_pool = ranking_df[(ranking_df['difficulty_level'] == inv_diff_order[v]) & (~ranking_df['course_id'].isin(already_selected_ids))]
            lower_selected = lower_pool.head(3 - sum(len(df) for df in final_selection))
            final_selection.append(lower_selected)
            if sum(len(df) for df in final_selection) >= 3: break

    current_count = sum(len(df) for df in final_selection)
    if current_count < 3:
        already_selected_ids = pd.concat(final_selection)['course_id'].tolist() if current_count > 0 else []
        global_pool = ranking_df[~ranking_df['course_id'].isin(already_selected_ids)]
        final_selection.append(global_pool.head(3 - current_count))
                
    raw_selection = pd.concat(final_selection)
    final_recs_df = raw_selection.sort_values(by=['churn_prob', 'transition_prob'], ascending=[True, False]).head(3).reset_index(drop=True)
    final_recs_df['rank'] = final_recs_df.index + 1
    return final_recs_df

# --- 3. EXPERT CHASE CATEGORY IDENTIFIER ---
def get_expert_chase_category(radar_df):
    available_to_chase = radar_df[radar_df['coverage_pct'] < 100].copy()
    if available_to_chase.empty: return None, 100
    
    expert_chase_df = available_to_chase.sort_values(by=['coverage_pct', 'course_count'], ascending=[False, True]).reset_index(drop=True)
    return expert_chase_df.iloc[0]['skill_category'], int(expert_chase_df.iloc[0]['coverage_pct'])

# --- MAIN LAMBDA HANDLER ---
def lambda_handler(event, context):
    raw_body = event.get('body', event)
    
    if isinstance(raw_body, str):
        body = json.loads(raw_body)
    else:
        body = raw_body

    # Force the user_id to an integer to match the DataFrame index as json elements are always strings
    try: 
        user_to_fetch = int(body.get('user_id'))  # Phase 1 input / Trigger 
        just_completed_course_id = int(body.get('just_completed_course_id'))  # Phase 1 input / Trigger 
    except (ValueError, TypeError): 
        return {   # If the keys are missing or not numbers, this will catch it
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid Input: user_id or course_id missing'})
        }
    
    requested_skill = body.get('skill_category')  # Phase 2 input / trigger



    # 1. Filter Data for specific User
    # The 'index' in your lifetime JSON is the user_id
    user_subset = df_lifetime_courses.loc[[user_to_fetch]]
    user_name = user_subset['user_name'].iloc[0]
    
    completed_courses = user_subset[user_subset['status'] == 'completed']['course_id'].tolist()
    enrolled_courses = user_subset['course_id'].tolist()
    
    # For demo purpose we picked random courses so we need to add it to total count.In prod only add to completed courses
    if just_completed_course_id: 
        if just_completed_course_id not in enrolled_courses:
            enrolled_courses.append(just_completed_course_id)
        if just_completed_course_id not in completed_courses:
            completed_courses.append(just_completed_course_id)

    # --- PHASE 2 ROUTER: If a specific skill category was clicked ---
    if requested_skill:
        # 1. Run the ranking logic for the category the user clicked on the chart
        category_ranking_df = category_ranking_logic(
            user_to_fetch, 
            just_completed_course_id, 
            requested_skill, 
            df_lifetime_courses, 
            completed_courses, 
            final_markov_matrix, 
            df_skill_categories, 
            df_courses
        )
        
        # 2. Extract the top 3 recommendations
        top_3_df = extract_top_3_recommendations(
            category_ranking_df, 
            just_completed_course_id, 
            df_courses
        )
        
        # 3. Add UI-specific metadata (Button text and Course Details)
        if not top_3_df.empty:
            top_3_df['button_text'] = top_3_df['course_id'].apply(
                lambda x: 'Continue' if x in enrolled_courses else 'Enroll Now'
            )
            # Add duration, rating, and url from global df_courses
            final_recs_df = top_3_df.merge(
                df_courses[['course_id', 'duration', 'rating', 'enroll_url']], 
                on='course_id', 
                how='left'
            )
            recommendations_list = final_recs_df.to_dict(orient='records')
        else:
            recommendations_list = []

        # 4. Immediate return for Phase 2 (skips Phase 1 radar logic)
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'skill_category': requested_skill,
                'recommendations': recommendations_list,
                'enrolled_courses': enrolled_courses  # Used by JS for button logic
            })
        }
    # --- END PHASE 2 ROUTER ---


    else:
        # --- PHASE 1: INITIAL PROFILE (RADAR + EXPERT CHASE) ---
        # 2. Compute Radar Chart Data
        skill_category_course_count = SKILL_CATEGORY_COURSE_COUNT
        user_completions = df_skill_categories[df_skill_categories['course_id'].isin(completed_courses)]
        user_category_counts = user_completions.groupby('skill_category').size().reset_index(name='user_count')

        radar_df = pd.merge(skill_category_course_count, user_category_counts, on='skill_category', how='left').fillna(0)
        radar_df['coverage_pct'] = (radar_df['user_count'] / radar_df['course_count']) * 100
        radar_df = radar_df.sort_values(by='coverage_pct', ascending=False).reset_index(drop=True)

        radar_df['visual_r'] = np.sqrt(radar_df['coverage_pct'] / 100) * 100
        radar_data = {
            "labels": radar_df['skill_category'].tolist(),
            "coverage": radar_df['coverage_pct'].astype(int).tolist(),
            "visual_r": radar_df['visual_r'].apply(lambda x: max(x, 8)).round(2).tolist(),
            "colors": ['#041ED1', '#FCA562', '#2EE86D', '#FC2B14', '#AF07DB', '#FF82A5', '#FCFA72', '#9E313C']
        }

        # 3. Execute Expert Chase Logic
        expert_target_cat, expert_pct = get_expert_chase_category(radar_df)
        expert_chase_dict = {}

        if expert_target_cat:
            expert_ranking_df = category_ranking_logic(user_to_fetch, just_completed_course_id, expert_target_cat, 
                                                    df_lifetime_courses, completed_courses, 
                                                    final_markov_matrix, df_skill_categories, df_courses)
            
            expert_top_3 = extract_top_3_recommendations(expert_ranking_df, just_completed_course_id, df_courses)
            
            if not expert_top_3.empty:
                expert_top_3['button_text'] = expert_top_3['course_id'].apply(lambda x: 'Continue' if x in enrolled_courses else 'Enroll Now')
                # Extract additional banner fields from df_courses
                banner_rec = expert_top_3.head(1).merge(df_courses[['course_id', 'duration', 'rating', 'enroll_url']], on='course_id', how='left')
                expert_chase_dict = banner_rec.to_dict(orient='records')[0]

        # 4. Return API Response
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                "user_name": user_name,
                "radar_data": radar_data,
                "expert_chase_dict": expert_chase_dict,
                "expert_target_category": expert_target_cat,
                "expert_pct": expert_pct
            })
        }