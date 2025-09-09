import os
import json
from openai import AzureOpenAI
from dotenv import load_dotenv

class SmartParameterGenerator:
    def __init__(self):
        # Load .env file from the root directory
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        load_dotenv(env_path)
        
        # Debug: Print if variables are loaded
        api_key = os.getenv("AZURE_OPENAI_API_KEY")
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        
        if not api_key:
            print("AZURE_OPENAI_API_KEY not found in environment")
            raise ValueError("Missing Azure OpenAI API key")
        
        if not endpoint:
            print("AZURE_OPENAI_ENDPOINT not found in environment")
            raise ValueError("Missing Azure OpenAI endpoint")
        
        print(f"API Key loaded: {api_key[:10]}...")
        print(f"Endpoint loaded: {endpoint}")
        
        self.client = AzureOpenAI(
            api_key=api_key,
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=endpoint
        )
        self.deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "turbo-2024-04-09")
    
    def validate_os_probabilities(self, os_dict):
        """Ensure OS probabilities sum to exactly 100%"""
        total = sum(os_dict.values())
        if abs(total - 100.0) > 0.1:  # Allow 0.1% tolerance
            # Proportionally adjust all values
            for key in os_dict:
                os_dict[key] = round((os_dict[key] * 100.0 / total), 1)
        return os_dict
    
    def validate_and_fix_parameters(self, params):
        """Apply validation and fixes to generated parameters"""
        try:
            # Fix Domain Controller OS probabilities
            if "DC" in params and "osProbability" in params["DC"]:
                params["DC"]["osProbability"] = self.validate_os_probabilities(params["DC"]["osProbability"])
            
            # Fix Computer OS probabilities
            if "Computer" in params and "osProbability" in params["Computer"]:
                params["Computer"]["osProbability"] = self.validate_os_probabilities(params["Computer"]["osProbability"])
            
            # Fix Domain functional level probabilities
            if "Domain" in params and "functionalLevelProbability" in params["Domain"]:
                params["Domain"]["functionalLevelProbability"] = self.validate_os_probabilities(params["Domain"]["functionalLevelProbability"])
            
            # Ensure session percentages follow scaling rules
            n_users = params.get("User", {}).get("nUsers", 0)
            if n_users > 0:
                if n_users <= 200:
                    correct_sessions = [2.0, 2.0, 2.0]
                elif n_users <= 500:
                    correct_sessions = [1.5, 1.5, 1.5]
                elif n_users <= 1000:
                    correct_sessions = [1.1, 1.1, 1.1]
                elif n_users <= 2000:
                    correct_sessions = [0.9, 0.9, 0.9]
                elif n_users <= 5000:
                    correct_sessions = [0.6, 0.6, 0.6]
                else:
                    correct_sessions = [0.21, 0.21, 0.21]
                
                if "User" in params:
                    params["User"]["sessionsPercentages"] = correct_sessions
            
            # Clean up misconfig arrays (ensure single values, not arrays)
            misconfig_keys = [
                "perc_misconfig_sessions", "perc_misconfig_permissions",
                "perc_misconfig_permissions_on_groups", "perc_misconfig_nesting_groups"
            ]
            for key in misconfig_keys:
                if key in params:
                    for subkey in ["Customized", "Low", "High"]:
                        if subkey in params[key] and isinstance(params[key][subkey], list):
                            params[key][subkey] = params[key][subkey][0] if params[key][subkey] else 0
            
            return params
            
        except Exception as e:
            print(f"Validation error: {e}")
            return params
    
    def generate_parameters(self, user_prompt):
        
        system_prompt = f"""
        You are an expert Active Directory security consultant. Generate COMPLETE ADSynth parameters in the EXACT format used by the system.

        CRITICAL REQUIREMENTS:
        1. **EXACT USER COUNT MATCHING**: When user specifies employee/user count, match it EXACTLY
        2. **OS PROBABILITY VALIDATION**: ALL OS probability dictionaries MUST sum to exactly 100%  
        3. **SECURITY LEVEL ACCURACY**: Misconfig percentages must reflect stated security posture
        4. **COMPUTER RATIO LOGIC**: Follow industry-appropriate computer-to-user ratios
        5. **SESSION SCALING**: Session percentages must decrease as organization size increases

        **USER COUNT ACCURACY (MOST CRITICAL):**
        - "200 employees" → nUsers: 200 (EXACTLY)
        - "1000 employees" → nUsers: 1000 (EXACTLY)  
        - "5000 employees" → nUsers: 5000 (EXACTLY)
        - "15000 employees" → nUsers: 15000 (EXACTLY)
        
        **COMPUTER-TO-USER RATIOS BY INDUSTRY:**
        - Office/Professional/Government: 0.8-1.2 computers per user
        - Manufacturing/Industrial: 0.3-0.6 computers per user  
        - Healthcare: 0.6-1.0 computers per user
        - Engineering/Tech: 0.9-1.3 computers per user
        - Retail: 0.3-0.8 computers per user
        - If user specifies exact ratio (e.g., "1:1"), match exactly

        **ORGANIZATION SIZE PATTERNS:**
        - Small (50-500): 0.6-1.0 computer ratio, higher session %
        - Medium (500-2000): 0.7-1.1 computer ratio, medium session %  
        - Large (2000-10000): 0.8-1.2 computer ratio, lower session %
        - Enterprise (10000+): 0.9-1.3 computer ratio, very low session %

        **SESSION PERCENTAGE SCALING (MANDATORY PATTERN):**
        - 50-200 users: [2.0, 2.0, 2.0]
        - 201-500 users: [1.5, 1.5, 1.5] 
        - 501-1000 users: [1.1, 1.1, 1.1]
        - 1001-2000 users: [0.9, 0.9, 0.9]
        - 2001-5000 users: [0.6, 0.6, 0.6]
        - 5000+ users: [0.21, 0.21, 0.21]

        **RESOURCE THRESHOLDS BY SIZE:**
        - Small (50-500): [3, 8]
        - Medium (500-2000): [80, 110] 
        - Large (2000-10000): [200, 210]
        - Enterprise (10000+): [500, 1100]

        **SECURITY LEVEL PATTERNS:**

        **MAXIMUM/HIGH SECURITY (Government, Military, Financial, Healthcare):**
        - perc_misconfig_sessions: 0.01-0.05% (Customized/Low), 0.1% (High)
        - perc_misconfig_permissions: 0.01-0.05% (Customized/Low), 0.1% (High)
        - perc_misconfig_permissions_on_groups: 0% (Customized/Low), 0.1% (High)  
        - perc_misconfig_nesting_groups: 0-0.1% (Customized/Low), 0.1% (High)
        - Windows 10 Enterprise: 60-85%
        - Server 2016 combined: 60-90%
        - LAPS coverage: 35-50%
        - exploitable: 5-15%
        - privesc: 5-15%
        - creddump: 5-15%

        **MEDIUM SECURITY (Standard Enterprise):**
        - perc_misconfig_sessions: 0.1-2% (Customized/Low), 5% (High)
        - perc_misconfig_permissions: 0.1-2% (Customized/Low), 5% (High)
        - perc_misconfig_permissions_on_groups: 0% (Customized/Low), 100% (High)
        - perc_misconfig_nesting_groups: 0-10% (Customized/Low), 20% (High)
        - Windows 10 Enterprise: 40-65%
        - Server 2016 combined: 40-70%
        - LAPS coverage: 20-35%
        - exploitable: 20-35%

        **BASIC/LOW SECURITY (Small business, budget constraints, minimal IT):**
        - perc_misconfig_sessions: 25-50% (Customized/Low), 5% (High)
        - perc_misconfig_permissions: 25-50% (Customized/Low), 5% (High)
        - perc_misconfig_permissions_on_groups: 20-40% (Customized/Low), 100% (High)
        - perc_misconfig_nesting_groups: 20-40% (Customized/Low), 20% (High)
        - Windows 10 Enterprise: 20-40%
        - Server 2016 combined: 35-55%
        - LAPS coverage: 5-20%
        - exploitable: 35-50%
        - Higher legacy system percentages (XP: 3-5%, Win7: 15-25%)

        **HEALTHCARE SPECIFIC ADJUSTMENTS:**
        - Treat as HIGH SECURITY by default
        - Windows 10 Enterprise: 60-80%
        - Server 2016 combined: 60-80% 
        - LAPS coverage: 35-50%
        - Very low misconfig percentages (0.01-0.1%)
        - Minimal legacy systems

        **BUDGET CONSTRAINT ADJUSTMENTS:**
        When "minimal IT budget", "budget constraints", "basic security" mentioned:
        - Reduce Windows 10 Enterprise by 20-30%
        - Reduce Server 2016 by 20-30%
        - Increase Windows 7/XP by 10-20%
        - Increase older server OS percentages
        - Reduce LAPS coverage significantly
        - Increase all misconfig rates by 15-25%

        **VALIDATION RULES:**
        1. ALL OS probability dictionaries MUST sum to exactly 100.0%
        2. If they don't, proportionally adjust all values: value_new = (value_old * 100 / total_sum)
        3. Round to 1 decimal place
        4. User count must EXACTLY match specification
        5. Computer ratio must match industry type or explicit requirements
        6. Session percentages must follow the mandatory scaling pattern above

        GENERATE THE COMPLETE JSON with ALL required sections:

        {{
            "Domain": {{
                "functionalLevelProbability": {{
                    "2008": [adjust based on security level],
                    "2008 R2": [adjust based on security level],
                    "2012": [adjust based on security level],
                    "2012 R2": [adjust based on security level],
                    "2016": [adjust based on security level - higher for secure orgs],
                    "Unknown": [adjust based on security level]
                }}
            }},
            "Computer": {{
                "nComputers": [calculate based on industry and size],
                "enabled": [80-90 for secure, 70-80 for basic],
                "haslaps": [35-50 for secure, 5-20 for basic],
                "unconstraineddelegation": [5-10 for secure, 10-15 for basic],
                "osProbability": {{
                    "Windows XP Professional Service Pack 3": [0-1 for secure, 3-5 for basic],
                    "Windows 7 Professional Service Pack 1": [2-5 for secure, 7-15 for basic],
                    "Windows 7 Ultimate Service Pack 1": [1-3 for secure, 5-10 for basic],
                    "Windows 7 Enterprise Service Pack 1": [5-10 for secure, 15-25 for basic],
                    "Windows 10 Pro": [15-25 for secure, 25-35 for basic],
                    "Windows 10 Enterprise": [60-85 for secure, 20-40 for basic]
                }},
                "privesc": [5-15 for secure, 30-50 for basic],
                "creddump": [5-15 for secure, 40-50 for basic],
                "exploitable": [5-15 for secure, 35-50 for basic],
                "computerProbability": {{
                    "PAW": [20-30 for secure, 10-20 for basic],
                    "Server": [20-30],
                    "Workstation": [45-60]
                }}
            }},
            "DC": {{
                "enabled": [95 for secure, 85-90 for basic],
                "haslaps": [35-50 for secure, 10-25 for basic],
                "osProbability": {{
                    "Windows Server 2003 Enterprise Edition": [0 for secure, 0.5-2 for basic],
                    "Windows Server 2008 Standard": [0-0.5 for secure, 1-3 for basic],
                    "Windows Server 2008 Datacenter": [0-0.5 for secure, 1-3 for basic],
                    "Windows Server 2008 Enterprise": [0-0.5 for secure, 1-3 for basic],
                    "Windows Server 2008 R2 Standard": [0.5-1 for secure, 2-5 for basic],
                    "Windows Server 2008 R2 Datacenter": [0.5-1.5 for secure, 3-6 for basic],
                    "Windows Server 2008 R2 Enterprise": [0.5-1.5 for secure, 3-6 for basic],
                    "Windows Server 2012 Standard": [1-3 for secure, 4-8 for basic],
                    "Windows Server 2012 Datacenter": [1-3 for secure, 4-8 for basic],
                    "Windows Server 2012 R2 Standard": [3-8 for secure, 8-15 for basic],
                    "Windows Server 2012 R2 Datacenter": [3-8 for secure, 8-15 for basic],
                    "Windows Server 2016 Standard": [30-45 for secure, 25-35 for basic],
                    "Windows Server 2016 Datacenter": [30-45 for secure, 20-30 for basic]
                }}
            }},
            "User": {{
                "nUsers": [EXACT match to user specification],
                "enabled": [98 for secure, 90-95 for basic],
                "dontreqpreauth": [1-2 for secure, 5-10 for basic],
                "hasspn": [3-8 for secure, 10-15 for basic],
                "passwordnotreqd": [1-2 for secure, 5-10 for basic],
                "pwdneverexpires": [5-15 for secure, 40-60 for basic],
                "sidhistory": [1-5 for secure, 10-15 for basic],
                "unconstraineddelegation": [1-5 for secure, 15-25 for basic],
                "savedcredentials": [5-15 for secure, 35-50 for basic],
                "Kerberoastable": [[1, 3] for secure, [3, 7] for basic],
                "sessionsPercentages": [use mandatory scaling pattern above],
                "priority_session_weight": 1,
                "perc_special_roles": [8-15 for secure, 10-15 for basic]
            }},
            "Group": {{
                "nestingGroupProbability": [5-15 for secure, 25-35 for basic],
                "departmentProbability": {{
                    "IT": 25,
                    "R&D": 25,
                    "BUSINESS": 25,
                    "HR": 25
                }},
                "nResourcesThresholds": [use size-based pattern above],
                "nLocalAdminsPerDepartment": [3, 5],
                "nOUsPerLocalAdmins": [3, 5],
                "nGroupsPerUsers": [3, 5]
            }},
            "GPO": {{
                "nGPOs": [35-50 for secure, 25-35 for basic],
                "exploitable": [5-15 for secure, 25-40 for basic]
            }},
            "ACLs": {{
                "ACLPrincipalsPercentage": [5-15 for secure, 25-35 for basic],
                "ACLsProbability": {{
                    "GenericAll": [0.5-5 for secure, 10-15 for basic],
                    "GenericWrite": [0.5-8 for secure, 15-20 for basic],
                    "WriteOwner": [0.5-8 for secure, 15-20 for basic],
                    "WriteDacl": [0.5-8 for secure, 15-20 for basic],
                    "AddMember": [1-15 for secure, 25-35 for basic],
                    "ForceChangePassword": [0.5-8 for secure, 15-20 for basic],
                    "AllExtendedRights": [0.5-5 for secure, 10-15 for basic]
                }}
            }},
            "perc_misconfig_sessions": {{
                "Customized": [use security level patterns above],
                "Low": [use security level patterns above],
                "High": [use security level patterns above]
            }},
            "perc_misconfig_permissions": {{
                "Customized": [use security level patterns above],
                "Low": [use security level patterns above],  
                "High": [use security level patterns above]
            }},
            "perc_misconfig_permissions_on_groups": {{
                "Customized": [use security level patterns above],
                "Low": [use security level patterns above],
                "High": [use security level patterns above]
            }},
            "perc_misconfig_nesting_groups": {{
                "Customized": [use security level patterns above],
                "Low": [use security level patterns above],
                "High": [use security level patterns above]
            }},
            "misconfig_permissions_to_tier_0": {{
                "allow": 1,
                "limit": [1 for secure, 3-5 for basic]
            }},
            "misconfig_group": {{
                "acl_ratio": [1-25 for secure, 40-60 for basic],
                "admin_ratio": [5-20 for secure, 25-40 for basic],
                "priority_paws_weight": 3
            }},
            "nTiers": 3,
            "Tier_1_Servers": {{
                "extraServers": []
            }},
            "Admin": {{
                "service_account": [5-10 for secure, 15-20 for basic],
                "Admin_Percentage": [8-15 for secure, 15-25 for basic]
            }},
            "nonACLs": {{
                "nonACLsPercentage": [5-10 for secure, 10-20 for basic],
                "nonACLsProbability": {{
                    "CanRDP": [5-15 for secure, 25-35 for basic],
                    "ExecuteDCOM": [5-15 for secure, 25-35 for basic],
                    "AllowedToDelegate": [5-15 for secure, 25-35 for basic],
                    "ReadLAPSPassword": [5-15 for secure, 25-35 for basic]
                }}
            }},
            "nodeMisconfig": {{
                "admin_regular": 0,
                "user_comp": 0
            }},
            "nLocations": [1-3 for secure/small, 5-10 for basic/distributed],
            "convert_to_directed_graphs": 0,
            "seed": 1,
            "graph_name": "[generate appropriate descriptive name]"
        }}

        **FINAL VALIDATION CHECKLIST:**
        Before outputting JSON, verify:
        1. User count EXACTLY matches specification
        2. ALL OS probability dictionaries sum to 100.0%
        3. Computer ratio appropriate for industry/size
        4. Session percentages follow mandatory scaling rules
        5. Security misconfig percentages align with stated security level
        6. All required sections present

        **INSTRUCTIONS:**
        1. Analyze user prompt for: organization size, industry, security level, budget constraints
        2. Apply appropriate patterns from above
        3. Generate complete JSON with ALL sections
        4. Validate all probability sums = 100%
        5. Return ONLY the JSON, no explanations

        User request: {user_prompt}
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Generate complete ADSynth parameters for: {user_prompt}"}
                ],
                temperature=0.7,
                max_tokens=4000
            )
            
            content = response.choices[0].message.content.strip()
            
            # Extract JSON from response
            start = content.find('{')
            end = content.rfind('}') + 1
            
            if start == -1 or end == 0:
                print("No valid JSON found in response")
                return None
                
            json_str = content[start:end]
            generated_params = json.loads(json_str)
            
            # Apply validation and fixes
            generated_params = self.validate_and_fix_parameters(generated_params)
            
            # Validate that all required sections are present
            required_sections = [
                "Domain", "Computer", "DC", "User", "Group", "GPO", "ACLs",
                "perc_misconfig_sessions", "perc_misconfig_permissions", 
                "perc_misconfig_permissions_on_groups", "perc_misconfig_nesting_groups",
                "misconfig_permissions_to_tier_0", "misconfig_group", "nTiers",
                "Tier_1_Servers", "Admin", "nonACLs", "nodeMisconfig", 
                "nLocations", "convert_to_directed_graphs", "seed"
            ]
            
            missing_sections = [section for section in required_sections if section not in generated_params]
            
            if missing_sections:
                print(f"Missing required sections: {missing_sections}")
                return None
            
            # Validate critical parameters
            n_users = generated_params.get("User", {}).get("nUsers", 0)
            n_computers = generated_params.get("Computer", {}).get("nComputers", 0)
            
            # Ensure values are integers, not lists
            if isinstance(n_users, list):
                n_users = n_users[0] if n_users else 0
                generated_params["User"]["nUsers"] = n_users
            if isinstance(n_computers, list):
                n_computers = n_computers[0] if n_computers else 0
                generated_params["Computer"]["nComputers"] = n_computers
                
            if (n_users > 0 and n_computers > 0):
                return generated_params
            else:
                print("Generated parameters validation failed - missing critical values")
                return None
            
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            print(f"Raw response: {content}")
            return None
        except Exception as e:
            print(f"Azure AI error: {e}")
            return None
