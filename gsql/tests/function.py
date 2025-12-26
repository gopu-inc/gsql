
#!/usr/bin/env python3
"""
Script de correction des f-strings avec backslash
Corrige automatiquement les f-strings problématiques dans le code Python
"""

import os
import re
import sys
from pathlib import Path

def find_problematic_fstrings(file_path):
    """Trouve les f-strings problématiques avec des backslashes"""
    patterns = [
        # F-strings avec des appels de fonction contenant des backslashes
        r'f"[^"]*\{[^}]*\\[^}]*\}[^"]*"',
        r"f'[^']*\{[^}]*\\[^}]*\}[^']*'",
        # F-strings multi-lignes avec des backslashes
        r'f"""[\s\S]*?\{[^}]*\\[^}]*\}[\s\S]*?"""',
        r"f'''[\s\S]*?\{[^}]*\\[^}]*\}[\s\S]*?'''",
    ]
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    problems = []
    for i, line in enumerate(content.split('\n'), 1):
        for pattern in patterns:
            if re.search(pattern, line):
                problems.append((i, line.strip()))
                break
    
    return problems

def fix_fstring_backslash(file_path):
    """Corrige les f-strings avec des backslashes"""
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    changes_made = False
    
    for i, line in enumerate(lines):
        # Recherche des patterns problématiques
        patterns_to_fix = [
            # Pattern: f"{theme.info(\"Exemple:\\\n  test\")}"
            (r'f(["\'])(.*?)\{([^}]+)\}(.*?)\1', r'f\1\2{\3}\4\1'),
        ]
        
        original_line = line
        for pattern, replacement in patterns_to_fix:
            line = re.sub(pattern, replacement, line)
        
        # Correction spécifique pour les backslashes dans les expressions f-string
        if 'f"' in line or "f'" in line:
            # Trouver toutes les expressions dans les f-strings
            fstring_pattern = r'f(["\'])(.*?)(?<!\\)\1'
            matches = list(re.finditer(fstring_pattern, line, re.DOTALL))
            
            for match in matches:
                quote = match.group(1)
                fstring_content = match.group(2)
                
                # Vérifier s'il y a des backslashes dans les expressions {}
                expr_pattern = r'\{(.*?)\}'
                expressions = re.findall(expr_pattern, fstring_content)
                
                for expr in expressions:
                    if '\\' in expr:
                        # Extraire le backslash problématique
                        if '\\n' in expr:
                            # Cas: \n dans l'expression
                            fixed_expr = expr.replace('\\n', '\\\\n')
                        elif '\\t' in expr:
                            # Cas: \t dans l'expression
                            fixed_expr = expr.replace('\\t', '\\\\t')
                        else:
                            # Cas général: échapper le backslash
                            fixed_expr = expr.replace('\\', '\\\\')
                        
                        # Remplacer dans le contenu
                        fstring_content = fstring_content.replace(
                            '{' + expr + '}', 
                            '{' + fixed_expr + '}'
                        )
                        changes_made = True
                
                # Reconstruire la ligne
                new_fstring = f'f{quote}{fstring_content}{quote}'
                line = line.replace(match.group(0), new_fstring)
        
        if line != original_line:
            lines[i] = line
            changes_made = True
    
    if changes_made:
        # Sauvegarder une copie de sauvegarde
        backup_path = file_path + '.backup'
        print(f"Création de la sauvegarde: {backup_path}")
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        # Écrire les corrections
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        print(f"Fichier corrigé: {file_path}")
        return True
    
    return False

def fix_specific_issue(file_path):
    """Correction spécifique pour le problème de la ligne 762"""
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Chercher le bloc problématique autour de la ligne 762
    for i in range(max(0, 750), min(len(lines), 780)):
        if 'epilog=f"""' in lines[i] or "epilog=f'''" in lines[i]:
            print(f"Trouvé f-string problématique à la ligne {i+1}")
            
            # Trouver la fin du f-string
            start_line = i
            for j in range(i, min(len(lines), i + 30)):
                if '"""' in lines[j] and j != i:
                    end_line = j
                    break
            else:
                end_line = i + 20
            
            # Récupérer le bloc
            block = ''.join(lines[start_line:end_line + 1])
            
            # Méthode 1: Convertir en string normale + format
            fixed_block = block.replace('f"""', '"""')
            
            # Remplacer les {theme.xxx("text")} par des appels formatés
            fixed_block = re.sub(
                r'\{theme\.(\w+)\(("[^"]*"|\'[^\']*\')\)\}',
                lambda m: '{" + theme.' + m.group(1) + '(' + m.group(2) + ') + "}',
                fixed_block
            )
            
            # Remplacer les autres variables
            fixed_block = re.sub(
                r'\{(?!theme\.)([^}]+)\}',
                lambda m: '{" + str(' + m.group(1) + ') + "}',
                fixed_block
            )
            
            # Remplacer le bloc dans les lignes
            lines[start_line:end_line + 1] = [fixed_block]
            
            print(f"Bloc corrigé des lignes {start_line+1} à {end_line+1}")
            
            # Sauvegarder et écrire
            backup_path = file_path + '.backup'
            print(f"Création de la sauvegarde: {backup_path}")
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            
            return True
    
    return False

def smart_fix_fstrings(file_path):
    """Correction intelligente des f-strings"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern pour trouver les f-strings multi-lignes problématiques
    pattern = r'(f"""[\s\S]*?\{[^}]*\\[^}]*\}[\s\S]*?""")'
    
    def replace_fstring(match):
        fstring = match.group(1)
        
        # Extraire le contenu sans f"""
        content = fstring[3:-3]
        
        # Trouver toutes les expressions avec backslash
        def replace_expr(m):
            expr = m.group(1)
            # Échapper les backslashes
            if '\\' in expr:
                # Cas particulier pour theme.info()
                if 'theme.' in expr and '(' in expr and ')' in expr:
                    # Extraire fonction et arguments
                    func_match = re.match(r'theme\.(\w+)\(([^)]+)\)', expr)
                    if func_match:
                        func_name = func_match.group(1)
                        args = func_match.group(2)
                        # Échapper les backslashes dans les arguments
                        args_fixed = args.replace('\\', '\\\\')
                        return f'{{theme.{func_name}({args_fixed})}}'
                
                # Échapper tous les backslashes
                return '{' + expr.replace('\\', '\\\\') + '}'
            return m.group(0)
        
        # Remplacer les expressions problématiques
        fixed_content = re.sub(r'\{(.*?)\}', replace_expr, content)
        
        return f'f"""\n{fixed_content}\n"""'
    
    # Appliquer la correction
    new_content = re.sub(pattern, replace_fstring, content)
    
    if new_content != content:
        # Sauvegarde
        backup_path = file_path + '.backup'
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        # Écrire les corrections
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print(f"Fichier corrigé: {file_path}")
        return True
    
    return False

def quick_fix(file_path):
    """Correction rapide et sûre"""
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    changes = []
    
    for i, line in enumerate(lines):
        original_line = line
        
        # Solution simple: remplacer les f-strings multi-lignes par des concaténations
        if 'epilog=f"""' in line or "epilog=f'''" in line:
            print(f"Correction de la ligne {i+1}: {line.strip()[:50]}...")
            
            # Trouver la fin du f-string
            start_idx = i
            for j in range(i, len(lines)):
                if '"""' in lines[j] and j > i:
                    end_idx = j
                    break
            else:
                end_idx = i
            
            # Construire un fix simple
            lines[i] = lines[i].replace('f"""', '"""')
            
            # Pour chaque ligne du bloc, remplacer {variable} par {variable}
            # (en enlevant juste le 'f' du début)
            for k in range(i, end_idx + 1):
                # Remplacer {theme.xxx("text")} par format()
                if 'theme.' in lines[k] and '{' in lines[k]:
                    lines[k] = re.sub(
                        r'\{(theme\.\w+\([^)]+\))\}',
                        lambda m: '{' + m.group(1).replace('\\', '\\\\') + '}',
                        lines[k]
                    )
            
            changes.append((i+1, original_line.strip()[:50]))
    
    if changes:
        # Sauvegarde
        backup_path = file_path + '.backup'
        print(f"\nCréation de la sauvegarde: {backup_path}")
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        # Écrire les corrections
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        print(f"\nCorrections appliquées:")
        for line_num, original in changes:
            print(f"  Ligne {line_num}: {original}...")
        
        return True
    
    return False

def analyze_file(file_path):
    """Analyse le fichier et propose des corrections"""
    print(f"\n{'='*60}")
    print(f"Analyse du fichier: {file_path}")
    print(f"{'='*60}")
    
    problems = find_problematic_fstrings(file_path)
    
    if problems:
        print(f"\nProblèmes trouvés: {len(problems)}")
        for line_num, line in problems:
            print(f"  Ligne {line_num}: {line}")
        
        print(f"\nOptions de correction:")
        print("  1. Correction automatique intelligente")
        print("  2. Correction spécifique (recommandé)")
        print("  3. Correction manuelle")
        print("  4. Annuler")
        
        choice = input("\nChoisissez une option (1-4): ").strip()
        
        if choice == '1':
            return smart_fix_fstrings(file_path)
        elif choice == '2':
            return fix_specific_issue(file_path)
        elif choice == '3':
            print("\nOuverture du fichier pour correction manuelle...")
            os.system(f"nano {file_path}")
            return True
        else:
            print("Annulation.")
            return False
    else:
        print("Aucun problème détecté.")
        return False

def main():
    """Fonction principale"""
    if len(sys.argv) != 2:
        print("Usage: python fix_fstrings.py <fichier.py>")
        print("Exemple: python fix_fstrings.py /root/gsql/gsql/__main__.py")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    if not os.path.exists(file_path):
        print(f"Fichier non trouvé: {file_path}")
        sys.exit(1)
    
    # Option 1: Correction rapide et sûre
    print("Application de la correction rapide...")
    if quick_fix(file_path):
        print("\n✓ Correction appliquée avec succès!")
        print(f"Une sauvegarde a été créée: {file_path}.backup")
        
        # Vérification
        print("\nVérification...")
        problems = find_problematic_fstrings(file_path)
        if not problems:
            print("✓ Aucun problème restant!")
        else:
            print(f"⚠ {len(problems)} problèmes restants.")
            for line_num, line in problems:
                print(f"  Ligne {line_num}: {line}")
    else:
        print("Aucune correction nécessaire ou problème non détecté.")
    
    # Option 2: Analyse détaillée
    print(f"\n{'='*60}")
    print("Pour une analyse plus détaillée:")
    print(f"python {sys.argv[0]} {file_path} --analyze")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
