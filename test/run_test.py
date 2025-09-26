#!/usr/bin/env python3
"""
Simple test runner for CDC batching functionality.
Run this script to test the CDC batching system.
"""

import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    print("🧪 CDC Batching Test Runner")
    print("=" * 40)
    print()
    print("Choose test type:")
    print("1. Main CDC Batching Test (5000 operations)")
    print("2. Forced Error Test (strict constraints)")
    print("3. Notifications Test (MS Teams)")
    print("4. All tests")
    print()
    
    choice = input("Enter choice (1/2/3/4): ").strip()
    
    if choice == "1":
        print("\n📋 Main CDC Batching Test:")
        print("1. Check if CDC process is running")
        print("2. Create test_table in MySQL")
        print("3. Perform 5000 INSERTs, 5000 UPDATEs, 5000 DELETEs")
        print("4. Verify data is properly batched in ClickHouse")
        print("5. Clean up test data")
        print()
        print("⚠️  IMPORTANT: Make sure CDC is running first!")
        print("   Start CDC with: python migres.py")
        print()
        
        response = input("Continue? (y/N): ").strip().lower()
        if response != 'y':
            print("Test cancelled.")
            return 0
        
        try:
            from test_cdc_batching import main as test_main
            return test_main()
        except ImportError as e:
            print(f"❌ Error importing test: {e}")
            return 1
        except Exception as e:
            print(f"❌ Test failed: {e}")
            return 1
            
    elif choice == "2":
        print("\n📋 Forced Error Test:")
        print("1. Create MySQL table with problematic data types")
        print("2. Insert data that will cause ClickHouse type conversion errors")
        print("3. Wait for CDC to process and fail")
        print("4. Check for error dump files in data/ directory")
        print("5. Clean up test data")
        print()
        print("⚠️  IMPORTANT: Make sure CDC is running first!")
        print("   Start CDC with: python migres.py")
        print()
        
        response = input("Continue? (y/N): ").strip().lower()
        if response != 'y':
            print("Test cancelled.")
            return 0
        
        try:
            from test_forced_errors import main as forced_error_test_main
            return forced_error_test_main()
        except Exception as e:
            print(f"\n💥 TEST ERROR: {e}")
            import traceback
            traceback.print_exc()
            return 1
    
    elif choice == "3":
        print("\n📋 Notifications Test:")
        print("1. Read notification configuration from config.yml")
        print("2. Test MS Teams webhook connectivity")
        print("3. Send test notifications (Error, Warning, Info)")
        print("4. Verify notifications appear in MS Teams channel")
        print()
        print("⚠️  IMPORTANT: Configure notifications in config.yml first!")
        print("   Add notifications section with webhook_url")
        print()
        
        response = input("Continue? (y/N): ").strip().lower()
        if response != 'y':
            print("Test cancelled.")
            return 0
        
        try:
            from test_notifications import test_notification_system
            return 0 if test_notification_system() else 1
        except ImportError as e:
            print(f"❌ Error importing test: {e}")
            return 1
        except Exception as e:
            print(f"❌ Test failed: {e}")
            return 1
    
    elif choice == "4":
        print("\n📋 Running All Tests:")
        print("1. Main CDC Batching Test")
        print("2. Forced Error Test")
        print("3. Notifications Test")
        print()
        print("⚠️  IMPORTANT: Make sure CDC is running first!")
        print("   Start CDC with: python migres.py")
        print()
        
        response = input("Continue? (y/N): ").strip().lower()
        if response != 'y':
            print("Test cancelled.")
            return 0
        
        try:
            # Run main test
            print("\n" + "="*60)
            print("🧪 RUNNING MAIN CDC BATCHING TEST")
            print("="*60)
            from test_cdc_batching import main as test_main
            main_result = test_main()
            
            if main_result != 0:
                print("❌ Main test failed, skipping other tests")
                return main_result
            
            # Run forced error test
            print("\n" + "="*60)
            print("🧪 RUNNING FORCED ERROR TEST")
            print("="*60)
            from test_forced_errors import main as forced_error_test_main
            forced_error_result = forced_error_test_main()
            
            if forced_error_result != 0:
                print("❌ Forced error test failed, skipping notifications test")
                return 1
            
            # Run notifications test
            print("\n" + "="*60)
            print("🧪 RUNNING NOTIFICATIONS TEST")
            print("="*60)
            from test_notifications import test_notification_system
            notifications_result = test_notification_system()
            
            if notifications_result:
                print("\n🎉 ALL TESTS PASSED!")
                return 0
            else:
                print("\n❌ Notifications test failed!")
                return 1
            
        except ImportError as e:
            print(f"❌ Error importing test: {e}")
            return 1
        except Exception as e:
            print(f"❌ Test failed: {e}")
            return 1
            
    else:
        print("❌ Invalid choice. Please enter 1, 2, 3, or 4.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

