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
    
    def generate_parameters(self, user_prompt):
        
        system_prompt = f"""
        You are an expert Active Directory security consultant. Generate COMPLETE ADSynth parameters in the EXACT format used by the system.

        CRITICAL SIZE ACCURACY:
        When user specifies employee/user count, match it EXACTLY:
        - "200 employees" → nUsers: 200
        - "1000 employees" → nUsers: 1000  
        - "5000 employees" → nUsers: 5000
        - "15000 employees" → nUsers: 15000
        
        Do NOT deviate from specified user counts. This is the most important requirement.

        ORGANIZATION SIZE PATTERNS:
        - Small (1k): 500-650 users, 150-300 computers
        - Medium (5k): 1800-2000 users, 1600 computers  
        - Large (10k): 3300 users, 3300 computers
        - Enterprise (50k): 15000-16600 users, 16600-25000 computers
        - Mega (100k): 33300+ users, 33300-35000 computers

        **HEALTHCARE SPECIFIC ADJUSTMENTS:**
        - Higher Windows 10 Enterprise: 60-80%
        - Higher Server 2016: 60-80% 
        - Better LAPS coverage: 25-40%
        - Lower legacy OS percentages
        - Very low misconfig percentages (0.02-0.1%)
        
        **SESSION PERCENTAGES BY SIZE:**
        - Small (1k): [2, 2, 2] or [0.2, 0.2, 0.2]  
        - Medium (5k): [0.9, 0.9, 0.9] or [1.1, 1.1, 1.1]
        - Large (10k): [0.6, 0.6, 0.6] or [0.9, 0.9, 0.9]
        - Enterprise (50k): [0.09, 0.09, 0.09] or [0.21, 0.21, 0.21]
        - Mega (100k): [0.09, 0.09, 0.09] or [0.11, 0.11, 0.11]
        
        **RESOURCE THRESHOLDS BY SIZE:**
        - Small (1k): [3, 8]
        - Medium (5k): [80, 110] 
        - Large (10k): [200, 210]
        - Enterprise (50k): [500, 1100]
        - Mega (100k): [1900, 2100]

        SECURITY LEVEL PATTERNS:
        **SECURE Organizations:**
        - misconfig_sessions: 0.02-4% (very low)
        - misconfig_permissions: 0.03-4% (very low)
        - misconfig_permissions_on_groups: 0%
        - misconfig_nesting_groups: 0-20%
        - Admin_Percentage: 10-30%
        - Modern OS emphasis (Server 2016: 25-50%)

        **VULNERABLE Organizations:**
        - misconfig_sessions: 30-86% (very high)
        - misconfig_permissions: 30-86% (very high) 
        - misconfig_permissions_on_groups: 30-50%
        - misconfig_nesting_groups: 10-50%
        - Admin_Percentage: 10-20%
        - More legacy systems

        GENERATE THE COMPLETE JSON with ALL required sections. Use this EXACT structure:

        {{
            "Domain": {{
                "functionalLevelProbability": {{
                    "2008": 4,
                    "2008 R2": 5,
                    "2012": 10,
                    "2012 R2": 30,
                    "2016": 50,
                    "Unknown": 1
                }}
            }},
            "Computer": {{
                "nComputers": [calculate based on size],
                "enabled": 80,
                "haslaps": 10,
                "unconstraineddelegation": 10,
                "osProbability": {{
                    "Windows XP Professional Service Pack 3": 3,
                    "Windows 7 Professional Service Pack 1": 7,
                    "Windows 7 Ultimate Service Pack 1": 5,
                    "Windows 7 Enterprise Service Pack 1": 15,
                    "Windows 10 Pro": 30,
                    "Windows 10 Enterprise": 40
                }},
                "privesc": 30,
                "creddump": 40,
                "exploitable": 40,
                "computerProbability": {{
                    "PAW": 20,
                    "Server": 20,
                    "Workstation": 60
                }}
            }},
            "DC": {{
                "enabled": 90,
                "haslaps": 10,
                "osProbability": {{
                    "Windows Server 2003 Enterprise Edition": 1,
                    "Windows Server 2008 Standard": 1,
                    "Windows Server 2008 Datacenter": 1,
                    "Windows Server 2008 Enterprise": 1,
                    "Windows Server 2008 R2 Standard": 2,
                    "Windows Server 2008 R2 Datacenter": 3,
                    "Windows Server 2008 R2 Enterprise": 3,
                    "Windows Server 2012 Standard": 4,
                    "Windows Server 2012 Datacenter": 4,
                    "Windows Server 2012 R2 Standard": 10,
                    "Windows Server 2012 R2 Datacenter": 10,
                    "Windows Server 2016 Standard": 35,
                    "Windows Server 2016 Datacenter": 25
                }}
            }},
            "User": {{
                "nUsers": [calculate based on size],
                "enabled": 95,
                "dontreqpreauth": 5,
                "hasspn": 10,
                "passwordnotreqd": 5,
                "pwdneverexpires": 50,
                "sidhistory": 10,
                "unconstraineddelegation": 20,
                "savedcredentials": 40,
                "Kerberoastable": [3, 5],
                "sessionsPercentages": [adjust based on size - use format like [0.9, 0.9, 0.9] for 3 tiers],
                "priority_session_weight": 1,
                "perc_special_roles": 10
            }},
            "Group": {{
                "nestingGroupProbability": 30,
                "departmentProbability": {{
                    "IT": 25,
                    "R&D": 25,
                    "BUSINESS": 25,
                    "HR": 25
                }},
                "nResourcesThresholds": [adjust based on size],
                "nLocalAdminsPerDepartment": [3, 5],
                "nOUsPerLocalAdmins": [3, 5],
                "nGroupsPerUsers": [3, 5]
            }},
            "GPO": {{
                "nGPOs": 30,
                "exploitable": 30
            }},
            "ACLs": {{
                "ACLPrincipalsPercentage": 30,
                "ACLsProbability": {{
                    "GenericAll": 10,
                    "GenericWrite": 15,
                    "WriteOwner": 15,
                    "WriteDacl": 15,
                    "AddMember": 30,
                    "ForceChangePassword": 15,
                    "AllExtendedRights": 10
                }}
            }},
            "perc_misconfig_sessions": {{
                "Customized": [set based on security level],
                "Low": [set based on security level],
                "High": 5
            }},
            "perc_misconfig_permissions": {{
                "Customized": [set based on security level],
                "Low": [set based on security level],
                "High": 5
            }},
            "perc_misconfig_permissions_on_groups": {{
                "Customized": [set based on security level],
                "Low": [set based on security level],
                "High": 100
            }},
            "perc_misconfig_nesting_groups": {{
                "Customized": [set based on security level],
                "Low": [set based on security level],
                "High": 20
            }},
            "misconfig_permissions_to_tier_0": {{
                "allow": 1,
                "limit": [1 for secure, higher for vulnerable]
            }},
            "misconfig_group": {{
                "acl_ratio": 50,
                "admin_ratio": 30,
                "priority_paws_weight": 3
            }},
            "nTiers": 3,
            "Tier_1_Servers": {{
                "extraServers": []
            }},
            "Admin": {{
                "service_account": 15,
                "Admin_Percentage": [set based on security level]
            }},
            "nonACLs": {{
                "nonACLsPercentage": 10,
                "nonACLsProbability": {{
                    "CanRDP": 25,
                    "ExecuteDCOM": 25,
                    "AllowedToDelegate": 25,
                    "ReadLAPSPassword": 25
                }}
            }},
            "nodeMisconfig": {{
                "admin_regular": 0,
                "user_comp": 0
            }},
            "nLocations": 5,
            "convert_to_directed_graphs": 0,
            "seed": 1,
            "graph_name": "[generate appropriate name]"
        }}

        CRITICAL: For healthcare organizations, enhance security parameters:
        1. Increase Windows 10 Enterprise to 60-80%
        2. Increase Server 2016 Datacenter/Standard to 60-80% total
        3. Increase haslaps to 25-40% 
        4. Reduce legacy OS percentages significantly
        5. Keep misconfig percentages very low (0.02-0.1%)

        INSTRUCTIONS:
        1. READ THE USER COUNT CAREFULLY - if they say "5000 employees", nUsers MUST be 5000
        2. Analyze the user prompt to determine organization size and security level
        3. Generate the COMPLETE JSON with ALL sections above
        4. Set nUsers to EXACTLY match the specified employee/user count
        5. Adjust nComputers based on size (typically 0.8-1.2 ratio to users)
        4. Adjust sessionsPercentages based on size (smaller = higher percentages)
        5. Adjust nResourcesThresholds based on size
        6. Set misconfig percentages based on security level (secure = low, vulnerable = high)
        7. Set Admin_Percentage appropriately
        8. Generate appropriate graph_name
        9. Return ONLY the complete JSON, no explanations

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
            
            # Clean up any arrays that should be single values
            def clean_misconfig_arrays(params):
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
            
            generated_params = clean_misconfig_arrays(generated_params)
            
            # Basic validation of critical values
            n_users = generated_params.get("User", {}).get("nUsers", 0)
            n_computers = generated_params.get("Computer", {}).get("nComputers", 0)
            
            # Ensure values are integers, not lists
            if isinstance(n_users, list):
                n_users = n_users[0] if n_users else 0
            if isinstance(n_computers, list):
                n_computers = n_computers[0] if n_computers else 0
                
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
