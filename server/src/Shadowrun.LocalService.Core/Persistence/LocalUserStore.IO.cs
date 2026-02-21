using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Web.Script.Serialization;

namespace Shadowrun.LocalService.Core.Persistence
{
    public sealed partial class LocalUserStore
    {
        private static ArrayList CoerceToArrayList(object value)
        {
            if (value == null)
            {
                return null;
            }

            var list = value as ArrayList;
            if (list != null)
            {
                return list;
            }

            var objArray = value as object[];
            if (objArray != null)
            {
                var created = new ArrayList(objArray.Length);
                for (var i = 0; i < objArray.Length; i++)
                {
                    created.Add(objArray[i]);
                }
                return created;
            }

            var enumerable = value as IEnumerable;
            if (enumerable != null)
            {
                var created = new ArrayList();
                foreach (var item in enumerable)
                {
                    created.Add(item);
                }
                return created;
            }

            return null;
        }

        private IDictionary LoadAccountNoThrow()
        {
            try
            {
                if (File.Exists(_accountPath))
                {
                    var json = File.ReadAllText(_accountPath, Encoding.UTF8);
                    var obj = Json.DeserializeObject(json) as IDictionary;
                    if (obj != null)
                    {
                        return obj;
                    }
                }
            }
            catch
            {
            }

            var fresh = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            fresh["IdentityHash"] = null;
            fresh["DisplayName"] = "OfflineRunner";
            fresh["Careers"] = null;
            fresh["LastCareerIndex"] = 0;
            return fresh;
        }

        private void SaveAccountNoThrow(IDictionary account)
        {
            try
            {
                var json = Json.Serialize(account);
                File.WriteAllText(_accountPath, json, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                try
                {
                    if (_logger != null)
                    {
                        _logger.Log(new
                        {
                            ts = RequestLogger.UtcNowIso(),
                            type = "persistence",
                            op = "save-account-failed",
                            path = _accountPath,
                            message = ex.Message,
                        });
                    }
                }
                catch
                {
                }
            }
        }

        private Dictionary<string, string> LoadSessionsNoThrow()
        {
            try
            {
                if (File.Exists(_sessionsPath))
                {
                    var json = File.ReadAllText(_sessionsPath, Encoding.UTF8);
                    var obj = Json.DeserializeObject(json) as IDictionary;
                    if (obj != null)
                    {
                        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        foreach (DictionaryEntry entry in obj)
                        {
                            var key = entry.Key as string;
                            var value = entry.Value as string;
                            if (!IsNullOrWhiteSpace(key) && !IsNullOrWhiteSpace(value))
                            {
                                dict[NormalizeGuidish(key)] = NormalizeGuidish(value);
                            }
                        }
                        return dict;
                    }
                }
            }
            catch
            {
            }

            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        private void SaveSessionsNoThrow(Dictionary<string, string> sessions)
        {
            try
            {
                var obj = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                foreach (var kvp in sessions)
                {
                    obj[kvp.Key] = kvp.Value;
                }
                var json = Json.Serialize(obj);
                File.WriteAllText(_sessionsPath, json, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                try
                {
                    if (_logger != null)
                    {
                        _logger.Log(new
                        {
                            ts = RequestLogger.UtcNowIso(),
                            type = "persistence",
                            op = "save-sessions-failed",
                            path = _sessionsPath,
                            message = ex.Message,
                        });
                    }
                }
                catch
                {
                }
            }
        }

        private IDictionary LoadPlayerInfoNoThrow()
        {
            try
            {
                if (File.Exists(_playerInfoPath))
                {
                    var json = File.ReadAllText(_playerInfoPath, Encoding.UTF8);
                    var obj = Json.DeserializeObject(json) as IDictionary;
                    if (obj != null)
                    {
                        return obj;
                    }
                }
            }
            catch
            {
            }

            return new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        }

        private void SavePlayerInfoNoThrow(IDictionary root)
        {
            try
            {
                var json = Json.Serialize(root);
                File.WriteAllText(_playerInfoPath, json, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                try
                {
                    if (_logger != null)
                    {
                        _logger.Log(new
                        {
                            ts = RequestLogger.UtcNowIso(),
                            type = "persistence",
                            op = "save-playerinfo-failed",
                            path = _playerInfoPath,
                            message = ex.Message,
                        });
                    }
                }
                catch
                {
                }
            }
        }

        private static JavaScriptSerializer CreateSerializer()
        {
            var serializer = new JavaScriptSerializer();
            serializer.MaxJsonLength = int.MaxValue;
            serializer.RecursionLimit = 100;
            return serializer;
        }

        private static string GetString(IDictionary dict, string key)
        {
            if (dict == null || IsNullOrWhiteSpace(key) || !dict.Contains(key) || dict[key] == null)
            {
                return null;
            }
            return dict[key] as string;
        }

        private static int GetInt(IDictionary dict, string key, int fallback)
        {
            if (dict == null || IsNullOrWhiteSpace(key) || !dict.Contains(key) || dict[key] == null)
            {
                return fallback;
            }
            try
            {
                return Convert.ToInt32(dict[key]);
            }
            catch
            {
                return fallback;
            }
        }

        private static IDictionary GetDict(IDictionary root, string key)
        {
            if (root == null || IsNullOrWhiteSpace(key) || !root.Contains(key))
            {
                return null;
            }
            return root[key] as IDictionary;
        }

        private static IDictionary GetOrCreateDict(IDictionary root, string key)
        {
            var existing = GetDict(root, key);
            if (existing != null)
            {
                return existing;
            }

            var created = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            root[key] = created;
            return created;
        }

        private static ArrayList BuildDefaultCareers(string identityHash)
        {
            var list = new ArrayList();
            for (var i = 0; i < 3; i++)
            {
                var slot = new CareerSlot();
                slot.Index = i;
                slot.IsOccupied = false;
                slot.CharacterName = string.Empty;
                slot.Portrait = string.Empty;
                slot.HubId = "Act01_HUB_02";
                slot.PendingPersistenceCreation = false;
                slot.CharacterIdentifier = NormalizeGuidish(identityHash) + ":" + i.ToString();
                list.Add(slot.ToDictionary());
            }
            return list;
        }

        private static bool IsGuidish(string value)
        {
            if (IsNullOrWhiteSpace(value))
            {
                return false;
            }
            try
            {
                new Guid(value.Trim());
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static string NormalizeGuidish(string value)
        {
            if (IsNullOrWhiteSpace(value))
            {
                return null;
            }
            try
            {
                var g = new Guid(value.Trim());
                return g.ToString();
            }
            catch
            {
                return value.Trim();
            }
        }

        private static bool IsNullOrWhiteSpace(string value)
        {
            return value == null || value.Trim().Length == 0;
        }
    }
}
