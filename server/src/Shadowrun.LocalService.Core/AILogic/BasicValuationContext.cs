using System;
using System.Collections.Generic;
using Cliffhanger.SRO.ServerClientCommons.ArtificialIntelligence;
using Cliffhanger.SRO.ServerClientCommons.Gameworld;
using SRO.Core.Compatibility.Math;

namespace Shadowrun.LocalService.Core.AILogic
{
    /// <summary>
    /// Minimal valuation context implementation.
    /// For now it exposes empty perceptions and provides conservative defaults.
    /// </summary>
    public sealed class BasicValuationContext : IValuationContext
    {
        private readonly IGameworldInstance _gameworld;
        private readonly IAgentSnapshot _protagonist;

        public BasicValuationContext(IGameworldInstance gameworld, IAgentSnapshot protagonist)
        {
            _gameworld = gameworld;
            _protagonist = protagonist;
            EnemySnapshots = new List<IAgentSnapshot>();
            PerceivedEnemies = new List<Entity>();
        }

        public IEnumerable<Entity> PerceivedEnemies { get; private set; }

        public ICollection<IAgentSnapshot> EnemySnapshots { get; private set; }

        public IAgentSnapshot ProtagonistSnapshot
        {
            get { return _protagonist; }
        }

        public IGameworldInstance Gameworld
        {
            get { return _gameworld; }
        }

        public float ChanceToHit(IntVector2D attackerPosition, int range, float accuracy, float toHitModifier, Entity target)
        {
            // Placeholder: without full combat calc context, return a neutral value.
            return 0.5f;
        }

        public bool IsWithinWalkRange(IntVector2D position)
        {
            // Placeholder: assume any position could be within walk range.
            return true;
        }

        public void Refresh()
        {
            // Placeholder: later populate perceived enemies, snapshots, reachable positions, etc.
        }
    }
}
