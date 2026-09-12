# Form layout tutorial screenshots

These PNGs were captured directly from a locally running build of
`src/CSharpDB.Admin/CSharpDB.Admin.csproj` on September 5, 2026 (Pacific time).
The Admin host embeds the actual `CSharpDB.Admin.Forms` designer and Data Entry
components. No mockup UI, generated image, or replacement HTML was used for
these captures.

The separate practice database contains one fictional customer: Maya,
`maya@example.com`, credit limit 1500, and example notes. The form is named
**Customer Details Practice**. It was built, bound, aligned, and saved through
Admin's normal UI. Images are browser captures of the designer, selected
Properties sections, or the rendered form area; the PNGs were copied unchanged
into this directory.

| Files | Actual Admin state |
| --- | --- |
| `designer-workspace.png` | Four label/input pairs; First name input selected. |
| `label-anchors.png`, `label-text.png` | First name label at 24, 24, 168, 32 with Top Left anchors and its Text caption. |
| `input-binding.png` | First Name (`first_name`) field with Two Way binding. |
| `align-before.png` | Four labels selected at uneven positions; Align toolbox exposed. |
| `align-after.png` | Same selection after clicking Left, then Dist V. |
| `anchor-properties.png` | First name input using Anchored / Stretch Across. |
| `stretch-both.png` | Notes Textarea temporarily set to Stretch Both to demonstrate its checkbox combination. |
| `entry-wide.png` | Saved Fixed form, 1212-pixel renderer; 972-pixel inputs. |
| `entry-narrow.png` | Same saved form, 862-pixel renderer; 622-pixel inputs. |
| `entry-stacked.png` | Same saved form, 234-pixel renderer; narrow stacked layout. |

The runtime captures use Top Left for labels and Stretch Across for every
input. `capture-manifest.json` records the original PNG dimensions and SHA-256
hashes checked against the browser capture files.

Step 9 includes a separately labeled interactive SVG example of anchor
behavior. Its width and height sliders control the tutorial example only;
Admin does not have these sliders. The example is not a screenshot.
